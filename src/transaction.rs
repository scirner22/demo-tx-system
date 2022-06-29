use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct ClientId(pub u16);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq)]
pub struct TxId(pub u32);

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum TransactionType {
    Chargeback,
    Deposit,
    Dispute,
    Resolve,
    Withdrawal,
}

#[derive(Debug, PartialEq)]
pub enum TransactionState {
    Open,
    ActiveDispute,
    ChargedBack,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::Open
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub _type: TransactionType,
    pub client: ClientId,
    pub tx: TxId,
    pub amount: Option<Decimal>, // 4 decimal precision is spelled out explicitly in the requirements
    #[serde(skip)]
    pub state: TransactionState,
}

impl Transaction {
    pub fn requires_unique_tx(&self) -> bool {
        match self._type {
            TransactionType::Withdrawal | TransactionType::Deposit => true,
            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => {
                false
            }
        }
    }

    pub fn requires_history(&self) -> bool {
        match self._type {
            TransactionType::Deposit => true,
            TransactionType::Withdrawal
            | TransactionType::Dispute
            | TransactionType::Resolve
            | TransactionType::Chargeback => false,
        }
    }
}
