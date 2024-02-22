#[rustfmt::skip]
#[path = "protobuf/sf.near.codec.v1.rs"]
pub mod pbcodec;

#[rustfmt::skip]
#[path = "protobuf/receipts.v1.rs"]
pub mod substreams_triggers;

use graph::{
    blockchain::{Block as BlockchainBlock, BlockHash, BlockPtr, BlockTime},
    prelude::{hex, web3::types::H256, BlockNumber},
};
use std::convert::TryFrom;
use std::fmt::LowerHex;

pub use pbcodec::*;

// TODO: Doing this for the types from aptos_protos will be a pain due to the orphan rule.

impl From<&CryptoHash> for H256 {
    fn from(input: &CryptoHash) -> Self {
        H256::from_slice(&input.bytes)
    }
}

impl LowerHex for &CryptoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(&self.bytes))
    }
}

impl BlockHeader {
    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        match (self.prev_hash.as_ref(), self.prev_height) {
            (Some(hash), number) => Some(BlockPtr::from((H256::from(hash), number))),
            _ => None,
        }
    }
}

impl<'a> From<&'a BlockHeader> for BlockPtr {
    fn from(b: &'a BlockHeader) -> BlockPtr {
        BlockPtr::from((H256::from(b.hash.as_ref().unwrap()), b.height))
    }
}

impl Block {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }

    pub fn ptr(&self) -> BlockPtr {
        BlockPtr::from(self.header())
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::from(b.header())
    }
}

impl BlockchainBlock for Block {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        let ts = i64::try_from(self.header().timestamp).unwrap();
        BlockTime::since_epoch(ts, 0)
    }
}

impl HeaderOnlyBlock {
    pub fn header(&self) -> &BlockHeader {
        self.header.as_ref().unwrap()
    }
}

impl<'a> From<&'a HeaderOnlyBlock> for BlockPtr {
    fn from(b: &'a HeaderOnlyBlock) -> BlockPtr {
        BlockPtr::from(b.header())
    }
}

impl BlockchainBlock for HeaderOnlyBlock {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().parent_ptr()
    }

    fn timestamp(&self) -> BlockTime {
        let ts = i64::try_from(self.header().timestamp).unwrap();
        BlockTime::since_epoch(ts, 0)
    }
}

impl execution_outcome::Status {
    pub fn is_success(&self) -> bool {
        use execution_outcome::Status::*;
        match self {
            Unknown(_) | Failure(_) => false,
            SuccessValue(_) | SuccessReceiptId(_) => true,
        }
    }
}

///////////////////
///////////////////
///////////////////
///////////////////
///////////////////
///////////////////

#[derive(Clone, Debug, Default)]
pub struct MyBlock {
    pub transactions: Vec<aptos_protos::transaction::v1::Transaction>,
}

// We don't properly chunk by block, a single "MyBlock" could have txns from multiple
// blocks. TODO: See if this is a problem.
impl BlockchainBlock for MyBlock {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.transactions[0].version).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        BlockPtr {
            hash: BlockHash(self.transactions[0].info.as_ref().unwrap().hash.clone().into_boxed_slice()),
            number: self.transactions[0].version as i32,
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        None
    }

    fn timestamp(&self) -> BlockTime {
        let ts = self.transactions[0].timestamp.as_ref().unwrap();
        BlockTime::since_epoch(ts.seconds, ts.nanos as u32)
    }
}
