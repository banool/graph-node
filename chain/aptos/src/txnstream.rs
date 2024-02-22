use aptos_protos::indexer::v1::{
    raw_data_client::RawDataClient, GetTransactionsRequest, TransactionsResponse,
};
use graph::{blockchain::block_stream::{BlockStream, BlockStreamError, BlockStreamEvent, BlockStreamMapper, BlockWithTriggers, FirehoseCursor, SUBSTREAMS_BUFFER_STREAM_SIZE}, data::subgraph::DeploymentHash};
use prost::Message;
use std::{marker::PhantomData, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};
use tonic::{Response, Streaming};
use tracing::{error, info}; // TODO: Use graph::slog
use url::Url;
use kanal::{AsyncReceiver, AsyncSender};
use graph::prelude::*;
use futures03::{Stream, StreamExt};
// use futures::stream::{Stream, StreamExt, FusedStream};
use graph::blockchain::Blockchain;

use crate::{codec::MyBlock, Chain};

/// GRPC request metadata key for the token ID.
const GRPC_API_GATEWAY_API_KEY_HEADER: &str = "authorization";
/// GRPC request metadata key for the request name. This is used to identify the
/// data destination.
const GRPC_REQUEST_NAME_HEADER: &str = "x-aptos-request-name";
/// GRPC connection id
const GRPC_CONNECTION_ID: &str = "x-aptos-connection-id";
/// We will try to reconnect to GRPC 5 times in case upstream connection is being updated
pub const RECONNECTION_MAX_RETRIES: u64 = 65;
/// 256MB
pub const MAX_RESPONSE_SIZE: usize = 1024 * 1024 * 256;

const BUFFER_SIZE: usize = 100;

pub struct TxnStreamer {
}

pub struct AptosTxnStream<C: Blockchain> {
    // TODO when creating this store the tokio task.
    // TODO reconcile the args normally passed to the stream builder with how this works
    //fixme: not sure if this is ok to be set as public, maybe
    // we do not want to expose the stream to the caller
    #[allow(dead_code)]
    streamer_task: tokio::task::JoinHandle<()>,
    receiver: AsyncReceiver<MyResp>,
    marker: PhantomData<C>,
}

impl <C> AptosTxnStream<C>
where
    C: Blockchain,
{
    pub fn new(
        deployment: DeploymentHash,
        // mapper: Arc<F>,
        // TODO: These two could be useful if we added upstream filtering support.
        // modules: Option<Modules>,
        // module_name: String,
        indexer_grpc_data_service_address: Url,
        auth_token: String,
        starting_version: Option<u64>,  // TODO: Handle this better.
        ending_version: Option<u64>,
        // logger: Logger,
        // registry: Arc<MetricsRegistry>,
    ) -> Self
    //where
    //    F: BlockStreamMapper<C> + 'static,
    {
        let (tx, receiver) = kanal::bounded_async::<MyResp>(BUFFER_SIZE);

        let indexer_grpc_http2_ping_interval = Duration::from_secs(30);
        let indexer_grpc_http2_ping_timeout = Duration::from_secs(10);

        let task = tokio::spawn(async move {
            create_fetcher_loop(
                tx,
                indexer_grpc_data_service_address,
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                starting_version.unwrap_or(0),
                ending_version,
                auth_token,
                "processorname".to_string(),
                starting_version.unwrap_or(0),
                BUFFER_SIZE,
            )
            .await
        });

        AptosTxnStream {
            streamer_task: task,
            receiver,
            marker: PhantomData::default(),
        }
    }
}

impl<C: Blockchain<Block = MyBlock>> Stream for AptosTxnStream<C> {
    type Item = Result<BlockStreamEvent<C>, BlockStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut stream = self.receiver.stream();
        // TODO Why doesn't normal poll_next work.
        match stream.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None), // No more items
            Poll::Ready(Some(my_resp)) => {
                let block_with_triggers = BlockWithTriggers {
                    block: MyBlock { transactions: my_resp.transactions },
                    trigger_data: vec![],  // TODO: What should this be?
                };
                let cursor = FirehoseCursor::from(None);
                let result = BlockStreamEvent::ProcessBlock(block_with_triggers, cursor);
                Poll::Ready(Some(Ok(result)))
            },
        }
    }
}

impl<C: Blockchain<Block = MyBlock>> BlockStream<C> for AptosTxnStream<C> {
    fn buffer_size_hint(&self) -> usize {
       BUFFER_SIZE
    }
}

fn grpc_request_builder(
    starting_version: u64,
    transactions_count: Option<u64>,
    grpc_auth_token: String,
    processor_name: String,
) -> tonic::Request<GetTransactionsRequest> {
    let mut request = tonic::Request::new(GetTransactionsRequest {
        starting_version: Some(starting_version),
        transactions_count,
        ..GetTransactionsRequest::default()
    });
    request.metadata_mut().insert(
        GRPC_API_GATEWAY_API_KEY_HEADER,
        format!("Bearer {}", grpc_auth_token.clone())
            .parse()
            .unwrap(),
    );
    request
        .metadata_mut()
        .insert(GRPC_REQUEST_NAME_HEADER, processor_name.parse().unwrap());
    request
}

async fn get_stream(
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    starting_version: u64,
    ending_version: Option<u64>,
    auth_token: String,
    processor_name: String,
) -> Response<Streaming<TransactionsResponse>> {
    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        "[Parser] Setting up rpc channel"
    );

    let channel = tonic::transport::Channel::from_shared(
        indexer_grpc_data_service_address.to_string(),
    )
    .expect(
        "[Parser] Failed to build GRPC channel, perhaps because the data service URL is invalid",
    )
    .http2_keep_alive_interval(indexer_grpc_http2_ping_interval)
    .keep_alive_timeout(indexer_grpc_http2_ping_timeout);

    // If the scheme is https, add a TLS config.
    let channel = if indexer_grpc_data_service_address.scheme() == "https" {
        let config = tonic::transport::channel::ClientTlsConfig::new();
        channel
            .tls_config(config)
            .expect("[Parser] Failed to create TLS config")
    } else {
        channel
    };

    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        "[Parser] Setting up GRPC client"
    );
    let mut rpc_client = match RawDataClient::connect(channel).await {
        Ok(client) => client
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(MAX_RESPONSE_SIZE)
            .max_encoding_message_size(MAX_RESPONSE_SIZE),
        Err(e) => {
            error!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                start_version = starting_version,
                ending_version = ending_version,
                error = ?e,
                "[Parser] Error connecting to GRPC client"
            );
            panic!("[Parser] Error connecting to GRPC client");
        },
    };
    let count = ending_version.map(|v| (v as i64 - starting_version as i64 + 1) as u64);
    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = ending_version,
        num_of_transactions = ?count,
        "[Parser] Setting up GRPC stream",
    );
    let request = grpc_request_builder(starting_version, count, auth_token, processor_name);
    rpc_client
        .get_transactions(request)
        .await
        .expect("[Parser] Failed to get grpc response. Is the server running?")
}

async fn get_chain_id(
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    auth_token: String,
    processor_name: String,
) -> u64 {
    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        "[Parser] Connecting to GRPC stream to get chain id",
    );
    let response = get_stream(
        indexer_grpc_data_service_address.clone(),
        indexer_grpc_http2_ping_interval,
        indexer_grpc_http2_ping_timeout,
        1,
        Some(2),
        auth_token.clone(),
        processor_name.to_string(),
    )
    .await;
    let connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
        Some(connection_id) => connection_id.to_str().unwrap().to_string(),
        None => "".to_string(),
    };
    let mut resp_stream = response.into_inner();
    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        connection_id,
        "[Parser] Successfully connected to GRPC stream to get chain id",
    );

    match resp_stream.next().await {
        Some(Ok(r)) => r.chain_id.expect("[Parser] Chain Id doesn't exist."),
        Some(Err(rpc_error)) => {
            error!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                error = ?rpc_error,
                "[Parser] Error receiving datastream response for chain id"
            );
            panic!("[Parser] Error receiving datastream response for chain id");
        },
        None => {
            error!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                "[Parser] Stream ended before getting response fo for chain id"
            );
            panic!("[Parser] Stream ended before getting response fo for chain id");
        },
    }
}

pub struct MyResp {
    transactions: Vec<aptos_protos::transaction::v1::Transaction>,
}

/// Gets a batch of transactions from the stream. Batch size is set in the grpc server.
/// The number of batches depends on our config
/// There could be several special scenarios:
/// 1. If we lose the connection, we will try reconnecting X times within Y seconds before crashing.
/// 2. If we specified an end version and we hit that, we will stop fetching, but we will make sure that
/// all existing transactions are processed
async fn create_fetcher_loop(
    txn_sender: AsyncSender<MyResp>,
    indexer_grpc_data_service_address: Url,
    indexer_grpc_http2_ping_interval: Duration,
    indexer_grpc_http2_ping_timeout: Duration,
    starting_version: u64,
    request_ending_version: Option<u64>,
    auth_token: String,
    processor_name: String,
    batch_start_version: u64,
    buffer_size: usize,
) {
    let mut grpc_channel_recv_latency = std::time::Instant::now();
    let mut next_version_to_fetch = batch_start_version;
    let mut reconnection_retries = 0;
    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        start_version = starting_version,
        end_version = request_ending_version,
        "[Parser] Connecting to GRPC stream",
    );
    let mut response = get_stream(
        indexer_grpc_data_service_address.clone(),
        indexer_grpc_http2_ping_interval,
        indexer_grpc_http2_ping_timeout,
        starting_version,
        request_ending_version,
        auth_token.clone(),
        processor_name.to_string(),
    )
    .await;
    let mut connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
        Some(connection_id) => connection_id.to_str().unwrap().to_string(),
        None => "".to_string(),
    };
    let mut resp_stream = response.into_inner();
    info!(
        processor_name = processor_name,
        service_type = "thegraphprocessor",
        stream_address = indexer_grpc_data_service_address.to_string(),
        connection_id,
        start_version = starting_version,
        end_version = request_ending_version,
        "[Parser] Successfully connected to GRPC stream",
    );

    let mut last_fetched_version = batch_start_version as i64 - 1;
    let mut batch_start_version = batch_start_version;
    loop {
        let is_success = match resp_stream.next().await {
            Some(Ok(r)) => {
                reconnection_retries = 0;
                let start_version = r.transactions.as_slice().first().unwrap().version;
                let start_txn_timestamp =
                    r.transactions.as_slice().first().unwrap().timestamp.clone();
                let end_version = r.transactions.as_slice().last().unwrap().version;
                let end_txn_timestamp = r.transactions.as_slice().last().unwrap().timestamp.clone();
                next_version_to_fetch = end_version + 1;
                let chain_id: u64 = r.chain_id.expect("[Parser] Chain Id doesn't exist.");

                let current_fetched_version = start_version;
                if last_fetched_version + 1 != current_fetched_version as i64 {
                    error!(
                        batch_start_version = batch_start_version,
                        last_fetched_version = last_fetched_version,
                        current_fetched_version = current_fetched_version,
                        "[Parser] Received batch with gap from GRPC stream"
                    );
                    panic!("[Parser] Received batch with gap from GRPC stream");
                }
                last_fetched_version = end_version as i64;
                batch_start_version = (last_fetched_version + 1) as u64;

                let txn_channel_send_latency = std::time::Instant::now();
                let txn_pb = MyResp {
                    transactions: r.transactions,
                };
                let duration_in_secs = txn_channel_send_latency.elapsed().as_secs_f64();
                let tps = (txn_pb.transactions.len() as f64
                    / txn_channel_send_latency.elapsed().as_secs_f64())
                    as u64;

                match txn_sender.send(txn_pb).await {
                    Ok(()) => {},
                    Err(e) => {
                        error!(
                            processor_name = processor_name,
                            stream_address = indexer_grpc_data_service_address.to_string(),
                            connection_id,
                            channel_size = buffer_size - txn_sender.capacity(),
                            error = ?e,
                            "[Parser] Error sending GRPC response to channel."
                        );
                        panic!("[Parser] Error sending GRPC response to channel.")
                    },
                }

                grpc_channel_recv_latency = std::time::Instant::now();
                true
            },
            Some(Err(rpc_error)) => {
                tracing::warn!(
                    processor_name = processor_name,
                    service_type = "thegraphprocessor",
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    start_version = starting_version,
                    end_version = request_ending_version,
                    error = ?rpc_error,
                    "[Parser] Error receiving datastream response."
                );
                false
            },
            None => {
                tracing::warn!(
                    processor_name = processor_name,
                    service_type = "thegraphprocessor",
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    start_version = starting_version,
                    end_version = request_ending_version,
                    "[Parser] Stream ended."
                );
                false
            },
        };
        // Check if we're at the end of the stream
        let is_end = if let Some(ending_version) = request_ending_version {
            next_version_to_fetch > ending_version
        } else {
            false
        };
        if is_end {
            info!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                ending_version = request_ending_version,
                next_version_to_fetch = next_version_to_fetch,
                "[Parser] Reached ending version.",
            );
            // Wait for the fetched transactions to finish processing before closing the channel
            loop {
                let channel_capacity = txn_sender.capacity();
                info!(
                    processor_name = processor_name,
                    service_type = "thegraphprocessor",
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    connection_id,
                    channel_size = buffer_size - channel_capacity,
                    "[Parser] Waiting for channel to be empty"
                );
                if channel_capacity == buffer_size {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            info!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                "[Parser] The stream is ended."
            );
            break;
        } else {
            // The rest is to see if we need to reconnect
            if is_success {
                continue;
            }

            // Sleep for 100ms between reconnect tries
            // TODO: Turn this into exponential backoff
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            if reconnection_retries >= RECONNECTION_MAX_RETRIES {
                error!(
                    processor_name = processor_name,
                    service_type = "thegraphprocessor",
                    stream_address = indexer_grpc_data_service_address.to_string(),
                    "[Parser] Reconnected more than 100 times. Will not retry.",
                );
                panic!("[Parser] Reconnected more than 100 times. Will not retry.")
            }
            reconnection_retries += 1;
            info!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                starting_version = next_version_to_fetch,
                ending_version = request_ending_version,
                reconnection_retries = reconnection_retries,
                "[Parser] Reconnecting to GRPC stream"
            );
            response = get_stream(
                indexer_grpc_data_service_address.clone(),
                indexer_grpc_http2_ping_interval,
                indexer_grpc_http2_ping_timeout,
                next_version_to_fetch,
                request_ending_version,
                auth_token.clone(),
                processor_name.to_string(),
            )
            .await;
            connection_id = match response.metadata().get(GRPC_CONNECTION_ID) {
                Some(connection_id) => connection_id.to_str().unwrap().to_string(),
                None => "".to_string(),
            };
            resp_stream = response.into_inner();
            info!(
                processor_name = processor_name,
                service_type = "thegraphprocessor",
                stream_address = indexer_grpc_data_service_address.to_string(),
                connection_id,
                starting_version = next_version_to_fetch,
                ending_version = request_ending_version,
                reconnection_retries = reconnection_retries,
                "[Parser] Successfully reconnected to GRPC stream"
            );
        }
    }
}
