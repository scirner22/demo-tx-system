use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

const DECIMAL_PRECISION: u32 = 4;

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
    pub amount: Option<Decimal>,
    #[serde(skip)]
    pub state: TransactionState,
}

impl Transaction {
    /// Returns a `bool` whether this transaction is valid. Negative numbers
    /// and `amount` precision in excess of four places after the decimal are considered invalid.
    /// Zero is determined to be a noop rather than an invalid, and greater precisions
    /// are not rounded due to the belief that if we're operating in a four place monetary system,
    /// any excess digits are more likely to represent a corrupted data point or an attempt
    /// at a buffer overlow attack.
    pub fn valid_tx_data(&self) -> bool {
        let amount = self.amount.unwrap_or_default();

        !amount.is_sign_negative() && amount.scale() <= DECIMAL_PRECISION
    }

    /// Returns a `bool` representing transaction types that should be tracked for global
    /// uniqueness. Put another way, transaction types that have tx pointers to existin txs cannot
    /// be unique.
    pub fn requires_unique_tx(&self) -> bool {
        match self._type {
            TransactionType::Withdrawal | TransactionType::Deposit => true,
            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => {
                false
            }
        }
    }

    /// Returns a `bool` representing transaction types that should be tracked so that other
    /// txs can reference them.
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

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial]
    fn valid_tx() {
        let actual = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::ONE),
            state: TransactionState::Open,
        };

        assert!(actual.valid_tx_data());
    }

    #[test]
    #[serial]
    fn valid_tx_boundary() {
        let actual = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::new(123456, 4)),
            state: TransactionState::Open,
        };

        assert!(actual.valid_tx_data());
    }

    #[test]
    #[serial]
    fn invalid_tx_boundary() {
        let actual = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::new(123456, 5)),
            state: TransactionState::Open,
        };

        assert!(!actual.valid_tx_data());
    }

    #[test]
    #[serial]
    fn invalid_tx() {
        let actual = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::new(123456789101112, 10)),
            state: TransactionState::Open,
        };

        assert!(!actual.valid_tx_data());
    }
}
