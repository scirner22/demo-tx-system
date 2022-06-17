use std::{collections::HashMap, env, error, io, path::Path};

use serde::{Serialize, Deserialize};

// TODO use Decimal crate for better match and 4 decimal precision

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
enum TransactionType {
    Chargeback,
    Deposit,
    Dispute,
    Resolve,
    Withdrawal,
}

#[derive(Debug, PartialEq)]
enum TransactionState {
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
struct Transaction {
    #[serde(rename = "type")]
    _type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f32>, // 4 decimal precision is spelled out explicitly in the requirements
    #[serde(skip)]
    state: TransactionState,
}

impl Transaction {
    fn requires_unique_tx(&self) -> bool {
        match self._type {
            TransactionType::Withdrawal | TransactionType::Deposit => true,
            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => false,
        }
    }

    fn requires_history(&self) -> bool {
        match self._type {
            TransactionType::Deposit => true,
            TransactionType::Withdrawal | TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq)]
struct Account {
    client: u16,
    available: f32,
    held: f32,
    total: f32, // available + held
    locked: bool, // an account is locked if a charge back occurs
    #[serde(skip)]
    touched: bool,
}

impl Account {
    fn is_locked_tx(&self, tx: &Transaction) -> bool {
        match tx._type {
            TransactionType::Deposit | TransactionType::Withdrawal if self.locked => true,
            TransactionType::Deposit | TransactionType::Withdrawal | TransactionType::Dispute | TransactionType::Chargeback | TransactionType::Resolve => false,
        }
    }

    fn apply_tx(&mut self, tx: &Transaction, referenced_tx: Option<&mut Transaction>) {
        self.touched = true;

        if self.is_locked_tx(tx) {
            return;
        }

        match tx._type {
            TransactionType::Deposit => {
                let amount = tx.amount.unwrap_or_default();

                self.available += amount;
                self.total += amount;
            },
            TransactionType::Withdrawal => {
                let amount = tx.amount.unwrap_or_default();

                if self.available >= amount {
                    self.available -= amount;
                    self.total -= amount;
                }
            },
            TransactionType::Dispute => {
                if let Some(referenced_tx) = referenced_tx {
                    match referenced_tx._type {
                        TransactionType::Deposit => {
                            match referenced_tx.state {
                                TransactionState::Open => {
                                    let amount = referenced_tx.amount.unwrap_or_default();

                                    if amount > 0f32 {
                                        referenced_tx.state = TransactionState::ActiveDispute;
                                        self.available -= amount;
                                        self.held += amount;
                                    }
                                },
                                TransactionState::ActiveDispute | TransactionState::ChargedBack => (),
                            }
                        },
                        TransactionType::Withdrawal | TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => (),
                    }
                }
            },
            TransactionType::Resolve => {
                if let Some(referenced_tx) = referenced_tx {
                    match referenced_tx._type {
                        TransactionType::Deposit => {
                            match referenced_tx.state {
                                TransactionState::ActiveDispute => {
                                    let amount = referenced_tx.amount.unwrap_or_default();

                                    if amount > 0f32 {
                                        self.available += amount;
                                        self.held -= amount;
                                        referenced_tx.state = TransactionState::Open;
                                    }
                                },
                                TransactionState::Open | TransactionState::ChargedBack => (),
                            }
                        },
                        TransactionType::Withdrawal | TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => (),
                    }
                }
            },
            TransactionType::Chargeback => {
                if let Some(referenced_tx) = referenced_tx {
                    match referenced_tx._type {
                        TransactionType::Deposit => {
                            match referenced_tx.state {
                                TransactionState::ActiveDispute => {
                                    let amount = referenced_tx.amount.unwrap_or_default();

                                    if amount > 0f32 {
                                        self.total -= amount;
                                        self.held -= amount;
                                        self.locked = true;
                                        referenced_tx.state = TransactionState::ChargedBack;
                                    }
                                },
                                TransactionState::Open | TransactionState::ChargedBack => (),
                            }
                        },
                        TransactionType::Withdrawal | TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => (),
                    }
                }
            },
        }
    }
}

pub fn run<P>(path: P) -> Result<(), Box<dyn error::Error>>
    where P: AsRef<Path>,
{
    // take advantage of client: u16 and allocate the full account ledger once
    let mut ledger: [Account; 65_536] = [Default::default(); 65_536]; // ~ 1MB allocation
    let mut tx_history: HashMap<u32, Transaction> = Default::default();

    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;

    for record in reader.deserialize() {
        let tx: Transaction = record?;

        if tx.requires_unique_tx() && tx_history.contains_key(&tx.tx) {
            let error = io::Error::new(
                io::ErrorKind::Other,
                "Withdrawal and Deposit TXs must be globally unique!",
            );

            return Err(Box::new(error));
        }

        let mut account = &mut ledger[tx.client as usize];
        account.client = tx.client;

        let referenced_tx = tx_history.get_mut(&tx.tx);

        let referenced_tx_client = referenced_tx.as_ref().map_or_else(|| tx.client, |x| x.client);

        // skip processing txs where the referenced tx is for a different client
        if referenced_tx_client == tx.client {
            account.apply_tx(&tx, referenced_tx);

            if tx.requires_history() {
                tx_history.insert(tx.tx, tx);
            }
        }
    }

    let mut wtr = csv::WriterBuilder::new()
        .from_writer(io::stdout());

    for account in &ledger {
        if account.touched {
            wtr.serialize(account)?;
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn error::Error>> {
    if let Some(arg) = env::args().nth(1) {
        run(arg)
    } else {
        let error = io::Error::new(
            io::ErrorKind::Other,
            "Must supply only a file path argument!",
        );

        Err(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn e2e() {
        let expected = std::fs::read_to_string("test_data/end_to_end_answer.csv").unwrap();
        let buf = gag::BufferRedirect::stdout().unwrap();
        let mut output = String::new();

        run("test_data/end_to_end.csv").unwrap();
        buf.into_inner().read_to_string(&mut output).unwrap();

        assert_eq!(expected, &output[..]);
    }

    #[test]
    fn simple_des() {
        let actual = r#"type, client, tx, amount
deposit,1,1,1.0
deposit, 2, 2, 2.0
deposit,     1, 3,                    2.0
withdrawal, 1, 4,    1.5
withdrawal, 2, 5, 3.0
chargeback, 1, 1,
dispute, 2, 2,
resolve, 2, 2,
"#;
        let mut actual = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(actual.as_bytes());
        let mut actual = actual.deserialize();
        let mut accum = Vec::default();

        while let Some(actual) = actual.next() {
            match actual {
                Ok::<Transaction, _>(actual) => accum.push(actual),
                Err(err) => assert_eq!(format!("{:?}", err), ""),
            }
        }

        assert_eq!(
            accum,
            vec![
                Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(1f32), state: TransactionState::Open },
                Transaction { _type: TransactionType::Deposit, client: 2u16, tx: 2u32, amount: Some(2f32), state: TransactionState::Open  },
                Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 3u32, amount: Some(2f32), state: TransactionState::Open  },
                Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 4u32, amount: Some(1.5f32), state: TransactionState::Open  },
                Transaction { _type: TransactionType::Withdrawal, client: 2u16, tx: 5u32, amount: Some(3.0f32), state: TransactionState::Open  },
                Transaction { _type: TransactionType::Chargeback, client: 1u16, tx: 1u32, amount: None, state: TransactionState::Open  },
                Transaction { _type: TransactionType::Dispute, client: 2u16, tx: 2u32, amount: None, state: TransactionState::Open  },
                Transaction { _type: TransactionType::Resolve, client: 2u16, tx: 2u32, amount: None, state: TransactionState::Open  },
            ],
        )
    }

    #[test]
    fn simple_ser() {
        let mut wtr = csv::WriterBuilder::new()
            .from_writer(vec![]);

        wtr.serialize(Account { client: 1u16, available: 1.5f32, held: 0f32, total: 1.5f32, locked: false, touched: true }).unwrap();
        wtr.serialize(Account { client: 2u16, available: 2f32, held: 0f32, total: 2f32, locked: true, touched: false }).unwrap();

        let actual = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
        let expected = r#"client,available,held,total,locked
1,1.5,0.0,1.5,false
2,2.0,0.0,2.0,true
"#;

        assert_eq!(actual, expected)
    }

    #[test]
    fn deposit_and_withdraw_flow() {
        let mut account = Account::default();

        let tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(1f32), state: TransactionState::Open };
        let tx2 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 2u32, amount: Some(3f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert!(account.touched);
        assert_eq!(4f32, account.total);
        assert_eq!(4f32, account.available);
        assert_eq!(0f32, account.held);

        let tx1 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 1u32, amount: Some(1f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);

        assert_eq!(3f32, account.total);
        assert_eq!(3f32, account.available);
        assert_eq!(0f32, account.held);

        let tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(5f32), state: TransactionState::Open };
        let tx2 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 2u32, amount: Some(1f32), state: TransactionState::Open };

        account.locked = true;
        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(3f32, account.total);
        assert_eq!(3f32, account.available);
        assert_eq!(0f32, account.held);
    }

    #[test]
    fn omit_excess_withdrawals() {
        let mut account = Account::default();

        let tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(1f32), state: TransactionState::Open };
        let tx2 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 2u32, amount: Some(3f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(1f32, account.total);
        assert_eq!(1f32, account.available);
        assert_eq!(0f32, account.held);
    }

    #[test]
    fn can_withdraw_to_zero() {
        let mut account = Account::default();

        let tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(10f32), state: TransactionState::Open };
        let tx2 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 2u32, amount: Some(10f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(0f32, account.total);
        assert_eq!(0f32, account.available);
        assert_eq!(0f32, account.held);
    }

    #[test]
    fn dispute_txs() {
        let mut account = Account::default();

        let mut tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(10f32), state: TransactionState::Open };
        let mut tx2 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 2u32, amount: Some(5f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);

        let dispute_tx = Transaction { _type: TransactionType::Dispute, client: 1u16, tx: 1u32, amount: None, state: TransactionState::Open };

        account.apply_tx(&dispute_tx, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);

        account.apply_tx(&dispute_tx, Some(&mut tx2));

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert_eq!(TransactionState::Open, tx2.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);
    }

    #[test]
    fn resolve_tx() {
        let mut account = Account::default();

        let mut tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(10f32), state: TransactionState::Open };
        let mut tx2 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 2u32, amount: Some(5f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);

        let dispute_tx = Transaction { _type: TransactionType::Dispute, client: 1u16, tx: 1u32, amount: None, state: TransactionState::Open };

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);

        let resolve_tx = Transaction { _type: TransactionType::Resolve, client: 1u16, tx: 1u32, amount: None, state: TransactionState::Open };

        tx2.state = TransactionState::ActiveDispute;
        account.apply_tx(&resolve_tx, Some(&mut tx2));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);

        account.apply_tx(&resolve_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert_eq!(TransactionState::Open, tx1.state);

        account.apply_tx(&resolve_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert_eq!(TransactionState::Open, tx1.state);
    }

    #[test]
    fn chargeback_tx() {
        let mut account = Account::default();

        let mut tx1 = Transaction { _type: TransactionType::Deposit, client: 1u16, tx: 1u32, amount: Some(10f32), state: TransactionState::Open };
        let mut tx2 = Transaction { _type: TransactionType::Withdrawal, client: 1u16, tx: 2u32, amount: Some(5f32), state: TransactionState::Open };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(!account.locked);

        let dispute_tx = Transaction { _type: TransactionType::Dispute, client: 1u16, tx: 1u32, amount: None, state: TransactionState::Open };

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);

        let chargeback_tx = Transaction { _type: TransactionType::Chargeback, client: 1u16, tx: 1u32, amount: None, state: TransactionState::Open };

        tx2.state = TransactionState::ActiveDispute;
        account.apply_tx(&chargeback_tx, Some(&mut tx2));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);

        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(-5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(account.locked);
        assert_eq!(TransactionState::ChargedBack, tx1.state);

        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(-5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(account.locked);
        assert_eq!(TransactionState::ChargedBack, tx1.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));
        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(-5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(account.locked);
        assert_eq!(TransactionState::ChargedBack, tx1.state);
    }
}

// optimizations
//
// -- serde allocation on every loop --
// test tests::medium ... bench: 205,058,347 ns/iter (+/- 14,764,710)
// test tests::small  ... bench:      96,385 ns/iter (+/- 4,975)
//
// -- zero serde allocations --
