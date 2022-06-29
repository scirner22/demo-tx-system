use rust_decimal::Decimal;
use serde::Serialize;

use crate::transaction::{ClientId, Transaction, TransactionState, TransactionType};

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq)]
pub struct Account {
    pub client: ClientId,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal, // available + held
    pub locked: bool,   // an account is locked if a charge back occurs
}

impl Account {
    pub fn new(client: ClientId) -> Self {
        Self {
            client,
            ..Default::default()
        }
    }

    fn is_locked_tx(&self, tx: &Transaction) -> bool {
        match tx._type {
            TransactionType::Deposit | TransactionType::Withdrawal if self.locked => true,
            TransactionType::Deposit
            | TransactionType::Withdrawal
            | TransactionType::Dispute
            | TransactionType::Chargeback
            | TransactionType::Resolve => false,
        }
    }

    pub fn apply_tx(&mut self, tx: &Transaction, referenced_tx: Option<&mut Transaction>) {
        if self.is_locked_tx(tx) {
            return;
        }

        match (&tx._type, referenced_tx.as_ref().map(|_ref| &_ref._type)) {
            (TransactionType::Deposit, _) => {
                let amount = tx.amount.unwrap_or_default();

                self.available += amount;
                self.total += amount;
            }
            (TransactionType::Withdrawal, _) => {
                let amount = tx.amount.unwrap_or_default();

                if self.available >= amount {
                    self.available -= amount;
                    self.total -= amount;
                }
            }
            (TransactionType::Dispute, Some(TransactionType::Deposit)) => {
                if let Some(referenced_tx) = referenced_tx {
                    match referenced_tx.state {
                        TransactionState::Open => {
                            let amount = referenced_tx.amount.unwrap_or_default();

                            if amount > Decimal::ZERO {
                                referenced_tx.state = TransactionState::ActiveDispute;
                                self.available -= amount;
                                self.held += amount;
                            }
                        }
                        TransactionState::ActiveDispute | TransactionState::ChargedBack => (),
                    }
                }
            }
            (TransactionType::Resolve, Some(TransactionType::Deposit)) => {
                if let Some(referenced_tx) = referenced_tx {
                    match referenced_tx.state {
                        TransactionState::ActiveDispute => {
                            let amount = referenced_tx.amount.unwrap_or_default();

                            if amount > Decimal::ZERO {
                                self.available += amount;
                                self.held -= amount;
                                referenced_tx.state = TransactionState::Open;
                            }
                        }
                        TransactionState::Open | TransactionState::ChargedBack => (),
                    }
                }
            }
            (TransactionType::Chargeback, Some(TransactionType::Deposit)) => {
                if let Some(referenced_tx) = referenced_tx {
                    match referenced_tx.state {
                        TransactionState::ActiveDispute => {
                            let amount = referenced_tx.amount.unwrap_or_default();

                            if amount > Decimal::ZERO {
                                self.total -= amount;
                                self.held -= amount;
                                self.locked = true;
                                referenced_tx.state = TransactionState::ChargedBack;
                            }
                        }
                        TransactionState::Open | TransactionState::ChargedBack => (),
                    }
                }
            }
            (TransactionType::Chargeback, _)
            | (TransactionType::Dispute, _)
            | (TransactionType::Resolve, _) => (),
        }
    }
}
