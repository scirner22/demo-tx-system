use std::{collections::HashMap, env, error, io, path::Path};

use serde::{Serialize, Deserialize};

// TODO use Decimal crate for better match and 4 decimal precision

const TX_TYPE_CHARGEBACK: &str = "chargeback";
const TX_TYPE_DEPOSIT: &str = "deposit";
const TX_TYPE_DISPUTE: &str = "dispute";
const TX_TYPE_RESOLVE: &str = "resolve";
const TX_TYPE_WITHDRAWAL: &str = "withdrawal";

const TX_STATE_OPEN: &str = "open";
const TX_STATE_ACTIVE_DISPUTE: &str = "active_dispute";
const TX_STATE_CHARGED_BACK: &str = "chargedback";

struct HistoricalTransaction {
    r#type: &'static str,
    client: u16,
    amount: f32,
    state: &'static str,
}

impl<'a> From<Transaction<'a>> for HistoricalTransaction {
    fn from(tx: Transaction) -> Self {
        Self {
            r#type: tx.type_to_static(),
            client: tx.client,
            amount: tx.amount.unwrap_or_default(),
            state: tx.state,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct Transaction<'a> {
    r#type: &'a str,
    client: u16,
    tx: u32,
    amount: Option<f32>, // 4 decimal precision is spelled out explicitly in the requirements
    #[serde(skip, default = "default_state")]
    state: &'static str,
}

fn default_state() -> &'static str {
    TX_STATE_OPEN
}

impl<'a> Transaction<'a> {
    fn type_to_static(&self) -> &'static str {
        match self.r#type {
            TX_TYPE_WITHDRAWAL => TX_TYPE_WITHDRAWAL,
            TX_TYPE_DEPOSIT => TX_TYPE_DEPOSIT,
            TX_TYPE_DISPUTE => TX_TYPE_DISPUTE,
            TX_TYPE_RESOLVE => TX_TYPE_RESOLVE,
            TX_TYPE_CHARGEBACK => TX_TYPE_CHARGEBACK,
            _ => "",
        }
    }

    fn requires_unique_tx(&self) -> bool {
        self.r#type == TX_TYPE_WITHDRAWAL || self.r#type == TX_TYPE_DEPOSIT
    }

    fn requires_history(&self) -> bool {
        self.r#type == TX_TYPE_DEPOSIT
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
        matches!(tx.r#type, TX_TYPE_DEPOSIT | TX_TYPE_WITHDRAWAL if self.locked)
    }

    fn apply_tx(&mut self, tx: &Transaction, referenced_tx: Option<&mut HistoricalTransaction>) {
        self.touched = true;

        if self.is_locked_tx(tx) {
            return;
        }

        match tx.r#type {
            TX_TYPE_DEPOSIT => {
                let amount = tx.amount.unwrap_or_default();

                self.available += amount;
                self.total += amount;
            },
            TX_TYPE_WITHDRAWAL => {
                let amount = tx.amount.unwrap_or_default();

                if self.available >= amount {
                    self.available -= amount;
                    self.total -= amount;
                }
            },
            TX_TYPE_DISPUTE => {
                if let Some(referenced_tx) = referenced_tx {
                    if referenced_tx.r#type == TX_TYPE_DEPOSIT && referenced_tx.state == TX_STATE_OPEN {
                        let amount = referenced_tx.amount;

                        if amount > 0f32 {
                            referenced_tx.state = TX_STATE_ACTIVE_DISPUTE;
                            self.available -= amount;
                            self.held += amount;
                        }
                    }
                }
            },
            TX_TYPE_RESOLVE => {
                if let Some(referenced_tx) = referenced_tx {
                    if referenced_tx.r#type == TX_TYPE_DEPOSIT && referenced_tx.state == TX_STATE_ACTIVE_DISPUTE {
                        let amount = referenced_tx.amount;

                        if amount > 0f32 {
                            self.available += amount;
                            self.held -= amount;
                            referenced_tx.state = TX_STATE_OPEN;
                        }
                    }
                }
            },
            TX_TYPE_CHARGEBACK => {
                if let Some(referenced_tx) = referenced_tx {
                    if referenced_tx.r#type == TX_TYPE_DEPOSIT && referenced_tx.state == TX_STATE_ACTIVE_DISPUTE {
                        let amount = referenced_tx.amount;

                        if amount > 0f32 {
                            self.total -= amount;
                            self.held -= amount;
                            self.locked = true;
                            referenced_tx.state = TX_STATE_CHARGED_BACK;
                        }
                    }
                }
            },
            _ => (),
        }
    }
}

pub fn run<P>(path: P) -> Result<(), Box<dyn error::Error>>
    where P: AsRef<Path>,
{
    // take advantage of client: u16 and allocate the full account ledger once
    let mut ledger: [Account; 65_536] = [Default::default(); 65_536]; // ~ 1MB allocation
    let mut tx_history: HashMap<u32, HistoricalTransaction> = Default::default();

    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;
    let mut raw_record = csv::StringRecord::new();
    let headers = reader.headers()?.clone();

    while reader.read_record(&mut raw_record)? {
        let tx: Transaction = raw_record.deserialize(Some(&headers))?;

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
                tx_history.insert(tx.tx, tx.into());
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
        let expected = vec![
            Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(1f32), state: TX_STATE_OPEN },
            Transaction { r#type: TX_TYPE_DEPOSIT, client: 2u16, tx: 2u32, amount: Some(2f32), state: TX_STATE_OPEN  },
            Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 3u32, amount: Some(2f32), state: TX_STATE_OPEN  },
            Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 4u32, amount: Some(1.5f32), state: TX_STATE_OPEN  },
            Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 2u16, tx: 5u32, amount: Some(3.0f32), state: TX_STATE_OPEN  },
            Transaction { r#type: TX_TYPE_CHARGEBACK, client: 1u16, tx: 1u32, amount: None, state: TX_STATE_OPEN  },
            Transaction { r#type: TX_TYPE_DISPUTE, client: 2u16, tx: 2u32, amount: None, state: TX_STATE_OPEN  },
            Transaction { r#type: TX_TYPE_RESOLVE, client: 2u16, tx: 2u32, amount: None, state: TX_STATE_OPEN  },
        ];
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
        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(actual.as_bytes());
        let mut raw_record = csv::StringRecord::new();
        let headers = reader.headers().unwrap().clone();
        let mut expected_counter = 0;

        // TODO fix
        while reader.read_record(&mut raw_record).unwrap() {
            let actual: Transaction = raw_record.deserialize(Some(&headers)).unwrap();

            assert_eq!(actual, expected[expected_counter]);

            expected_counter += 1;
        }
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

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(1f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 2u32, amount: Some(3f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert!(account.touched);
        assert_eq!(4f32, account.total);
        assert_eq!(4f32, account.available);
        assert_eq!(0f32, account.held);

        let tx1 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 1u32, amount: Some(1f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);

        assert_eq!(3f32, account.total);
        assert_eq!(3f32, account.available);
        assert_eq!(0f32, account.held);

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(5f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 2u32, amount: Some(1f32), state: TX_STATE_OPEN };

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

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(1f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 2u32, amount: Some(3f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(1f32, account.total);
        assert_eq!(1f32, account.available);
        assert_eq!(0f32, account.held);
    }

    #[test]
    fn can_withdraw_to_zero() {
        let mut account = Account::default();

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(10f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 2u32, amount: Some(10f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(0f32, account.total);
        assert_eq!(0f32, account.available);
        assert_eq!(0f32, account.held);
    }

    #[test]
    fn dispute_txs() {
        let mut account = Account::default();

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(10f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 2u32, amount: Some(5f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);

        let dispute_tx = Transaction { r#type: TX_TYPE_DISPUTE, client: 1u16, tx: 1u32, amount: None, state: TX_STATE_OPEN };

        account.apply_tx(&dispute_tx, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);

        let mut tx1 = tx1.into();
        let mut tx2 = tx2.into();

        account.apply_tx(&dispute_tx, Some(&mut tx2));

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert_eq!(TX_STATE_OPEN, tx2.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TX_STATE_ACTIVE_DISPUTE, tx1.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TX_STATE_ACTIVE_DISPUTE, tx1.state);
    }

    #[test]
    fn resolve_tx() {
        let mut account = Account::default();

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(10f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 2u32, amount: Some(5f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);

        let dispute_tx = Transaction { r#type: TX_TYPE_DISPUTE, client: 1u16, tx: 1u32, amount: None, state: TX_STATE_OPEN };
        let mut tx1 = tx1.into();
        let mut tx2: HistoricalTransaction = tx2.into();

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TX_STATE_ACTIVE_DISPUTE, tx1.state);

        let resolve_tx = Transaction { r#type: TX_TYPE_RESOLVE, client: 1u16, tx: 1u32, amount: None, state: TX_STATE_OPEN };

        tx2.state = TX_STATE_ACTIVE_DISPUTE;
        account.apply_tx(&resolve_tx, Some(&mut tx2));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);

        account.apply_tx(&resolve_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert_eq!(TX_STATE_OPEN, tx1.state);

        account.apply_tx(&resolve_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert_eq!(TX_STATE_OPEN, tx1.state);
    }

    #[test]
    fn chargeback_tx() {
        let mut account = Account::default();

        let tx1 = Transaction { r#type: TX_TYPE_DEPOSIT, client: 1u16, tx: 1u32, amount: Some(10f32), state: TX_STATE_OPEN };
        let tx2 = Transaction { r#type: TX_TYPE_WITHDRAWAL, client: 1u16, tx: 2u32, amount: Some(5f32), state: TX_STATE_OPEN };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(5f32, account.total);
        assert_eq!(5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(!account.locked);

        let dispute_tx = Transaction { r#type: TX_TYPE_DISPUTE, client: 1u16, tx: 1u32, amount: None, state: TX_STATE_OPEN };
        let mut tx1 = tx1.into();
        let mut tx2: HistoricalTransaction = tx2.into();

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);
        assert_eq!(TX_STATE_ACTIVE_DISPUTE, tx1.state);

        let chargeback_tx = Transaction { r#type: TX_TYPE_CHARGEBACK, client: 1u16, tx: 1u32, amount: None, state: TX_STATE_OPEN };

        tx2.state = TX_STATE_ACTIVE_DISPUTE;
        account.apply_tx(&chargeback_tx, Some(&mut tx2));

        assert_eq!(5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(10f32, account.held);

        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(-5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(account.locked);
        assert_eq!(TX_STATE_CHARGED_BACK, tx1.state);

        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(-5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(account.locked);
        assert_eq!(TX_STATE_CHARGED_BACK, tx1.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));
        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(-5f32, account.total);
        assert_eq!(-5f32, account.available);
        assert_eq!(0f32, account.held);
        assert!(account.locked);
        assert_eq!(TX_STATE_CHARGED_BACK, tx1.state);
    }
}

// optimizations
//
// -- serde allocation on every loop --
// test tests::medium ... bench: 205,058,347 ns/iter (+/- 14,764,710)
// test tests::small  ... bench:      96,385 ns/iter (+/- 4,975)
//
// -- zero serde allocations --
// nearly the same as above..based on the flamegraph the majority of time is being spent in
// csv::trim and csv::StringRecord
