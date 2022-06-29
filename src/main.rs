mod account;
mod transaction;

use std::{collections::HashMap, env, error, io, path::Path};

use account::Account;
use transaction::{Transaction, TxId};

pub fn run<P>(path: P) -> Result<(), Box<dyn error::Error>>
where
    P: AsRef<Path>,
{
    let mut ledger = HashMap::new();
    let mut tx_history: HashMap<TxId, Transaction> = Default::default();

    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;

    for record in reader.deserialize() {
        let tx: Transaction = record?;

        let amount = tx.amount.unwrap_or_default();

        // skip transactions with an invalid amount
        if amount.is_sign_negative() {
            continue;
        }

        if tx.requires_unique_tx() && tx_history.contains_key(&tx.tx) {
            let error = io::Error::new(
                io::ErrorKind::Other,
                "Withdrawal and Deposit TXs must be globally unique!",
            );

            return Err(Box::new(error));
        }

        let account = ledger
            .entry(tx.client)
            .or_insert_with(|| Account::new(tx.client));
        let referenced_tx = tx_history.get_mut(&tx.tx);
        let referenced_tx_client = referenced_tx
            .as_ref()
            .map_or_else(|| tx.client, |x| x.client);

        // skip processing txs where the referenced tx is for a different client
        if referenced_tx_client == tx.client {
            account.apply_tx(&tx, referenced_tx);

            if tx.requires_history() {
                tx_history.insert(tx.tx, tx);
            }
        }
    }

    let mut wtr = csv::WriterBuilder::new().from_writer(io::stdout());

    for account in ledger.values() {
        wtr.serialize(account)?;
    }

    wtr.flush()?;

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

    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serial_test::serial;

    use super::*;
    use crate::transaction::{ClientId, TransactionState, TransactionType, TxId};

    #[test]
    #[serial]
    fn e2e() {
        let expected1 =
            "client,available,held,total,locked\n2,0,0,0,true\n1,0.5000,1.0111,1.5111,false\n";
        let expected2 =
            "client,available,held,total,locked\n1,0.5000,1.0111,1.5111,false\n2,0,0,0,true\n";
        let buf = gag::BufferRedirect::stdout().unwrap();
        let mut output = String::new();

        run("test_data/end_to_end.csv").unwrap();
        buf.into_inner().read_to_string(&mut output).unwrap();

        if &output[..] != expected1 && &output[..] != expected2 {
            assert_eq!("", &output[..]);
        }
    }

    #[test]
    #[serial]
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
                Transaction {
                    _type: TransactionType::Deposit,
                    client: ClientId(1u16),
                    tx: TxId(1u32),
                    amount: Some(Decimal::ONE),
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Deposit,
                    client: ClientId(2u16),
                    tx: TxId(2u32),
                    amount: Some(Decimal::TWO),
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Deposit,
                    client: ClientId(1u16),
                    tx: TxId(3u32),
                    amount: Some(Decimal::TWO),
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Withdrawal,
                    client: ClientId(1u16),
                    tx: TxId(4u32),
                    amount: Some(dec!(1.5)),
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Withdrawal,
                    client: ClientId(2u16),
                    tx: TxId(5u32),
                    amount: Some(dec!(3.0)),
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Chargeback,
                    client: ClientId(1u16),
                    tx: TxId(1u32),
                    amount: None,
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Dispute,
                    client: ClientId(2u16),
                    tx: TxId(2u32),
                    amount: None,
                    state: TransactionState::Open
                },
                Transaction {
                    _type: TransactionType::Resolve,
                    client: ClientId(2u16),
                    tx: TxId(2u32),
                    amount: None,
                    state: TransactionState::Open
                },
            ],
        )
    }

    #[test]
    #[serial]
    fn simple_ser() {
        let mut wtr = csv::WriterBuilder::new().from_writer(vec![]);

        wtr.serialize(Account {
            client: ClientId(1u16),
            available: dec!(1.5),
            held: Decimal::ZERO,
            total: dec!(1.5),
            locked: false,
        })
        .unwrap();
        wtr.serialize(Account {
            client: ClientId(2u16),
            available: Decimal::TWO,
            held: Decimal::ZERO,
            total: Decimal::TWO,
            locked: true,
        })
        .unwrap();

        let actual = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
        let expected = r#"client,available,held,total,locked
1,1.5,0,1.5,false
2,2,0,2,true
"#;

        assert_eq!(actual, expected)
    }

    #[test]
    #[serial]
    fn deposit_and_withdraw_flow() {
        let mut account = Account::default();

        let tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::ONE),
            state: TransactionState::Open,
        };
        let tx2 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(dec!(3)),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(dec!(4), account.total);
        assert_eq!(dec!(4), account.available);
        assert_eq!(Decimal::ZERO, account.held);

        let tx1 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::ONE),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);

        assert_eq!(dec!(3), account.total);
        assert_eq!(dec!(3), account.available);
        assert_eq!(Decimal::ZERO, account.held);

        let tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(dec!(5)),
            state: TransactionState::Open,
        };
        let tx2 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(Decimal::ONE),
            state: TransactionState::Open,
        };

        account.locked = true;
        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(dec!(3), account.total);
        assert_eq!(dec!(3), account.available);
        assert_eq!(Decimal::ZERO, account.held);
    }

    #[test]
    #[serial]
    fn omit_excess_withdrawals() {
        let mut account = Account::default();

        let tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(Decimal::ONE),
            state: TransactionState::Open,
        };
        let tx2 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(dec!(3)),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(Decimal::ONE, account.total);
        assert_eq!(Decimal::ONE, account.available);
        assert_eq!(Decimal::ZERO, account.held);
    }

    #[test]
    #[serial]
    fn can_withdraw_to_zero() {
        let mut account = Account::default();

        let tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(dec!(10)),
            state: TransactionState::Open,
        };
        let tx2 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(dec!(10)),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(Decimal::ZERO, account.total);
        assert_eq!(Decimal::ZERO, account.available);
        assert_eq!(Decimal::ZERO, account.held);
    }

    #[test]
    #[serial]
    fn dispute_txs() {
        let mut account = Account::default();

        let mut tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(dec!(10)),
            state: TransactionState::Open,
        };
        let mut tx2 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(dec!(5)),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);

        let dispute_tx = Transaction {
            _type: TransactionType::Dispute,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: None,
            state: TransactionState::Open,
        };

        account.apply_tx(&dispute_tx, None);

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);

        account.apply_tx(&dispute_tx, Some(&mut tx2));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(TransactionState::Open, tx2.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(dec!(10), account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(dec!(10), account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);
    }

    #[test]
    #[serial]
    fn resolve_tx() {
        let mut account = Account::default();

        let mut tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(dec!(10)),
            state: TransactionState::Open,
        };
        let mut tx2 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(dec!(5)),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);

        let dispute_tx = Transaction {
            _type: TransactionType::Dispute,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: None,
            state: TransactionState::Open,
        };

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(dec!(10), account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);

        let resolve_tx = Transaction {
            _type: TransactionType::Resolve,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: None,
            state: TransactionState::Open,
        };

        tx2.state = TransactionState::ActiveDispute;
        account.apply_tx(&resolve_tx, Some(&mut tx2));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(dec!(10), account.held);

        account.apply_tx(&resolve_tx, Some(&mut tx1));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(TransactionState::Open, tx1.state);

        account.apply_tx(&resolve_tx, Some(&mut tx1));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert_eq!(TransactionState::Open, tx1.state);
    }

    #[test]
    #[serial]
    fn chargeback_tx() {
        let mut account = Account::default();

        let mut tx1 = Transaction {
            _type: TransactionType::Deposit,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: Some(dec!(10)),
            state: TransactionState::Open,
        };
        let mut tx2 = Transaction {
            _type: TransactionType::Withdrawal,
            client: ClientId(1u16),
            tx: TxId(2u32),
            amount: Some(dec!(5)),
            state: TransactionState::Open,
        };

        account.apply_tx(&tx1, None);
        account.apply_tx(&tx2, None);

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert!(!account.locked);

        let dispute_tx = Transaction {
            _type: TransactionType::Dispute,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: None,
            state: TransactionState::Open,
        };

        account.apply_tx(&dispute_tx, Some(&mut tx1));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(dec!(10), account.held);
        assert_eq!(TransactionState::ActiveDispute, tx1.state);

        let chargeback_tx = Transaction {
            _type: TransactionType::Chargeback,
            client: ClientId(1u16),
            tx: TxId(1u32),
            amount: None,
            state: TransactionState::Open,
        };

        tx2.state = TransactionState::ActiveDispute;
        account.apply_tx(&chargeback_tx, Some(&mut tx2));

        assert_eq!(dec!(5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(dec!(10), account.held);

        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(dec!(-5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert!(account.locked);
        assert_eq!(TransactionState::ChargedBack, tx1.state);

        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(dec!(-5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
        assert!(account.locked);
        assert_eq!(TransactionState::ChargedBack, tx1.state);

        account.apply_tx(&dispute_tx, Some(&mut tx1));
        account.apply_tx(&chargeback_tx, Some(&mut tx1));

        assert_eq!(dec!(-5), account.total);
        assert_eq!(dec!(-5), account.available);
        assert_eq!(Decimal::ZERO, account.held);
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
// no speed up at all - after looking at the flamegraph it's apparent the majority of the time is
// spend in csv::trim and csv::StringRecord
//
// -- serde allocation + moving f32 to Decimal - final version
// test tests::medium ... bench: 231,653,952 ns/iter (+/- 18,252,282)
// test tests::small  ... bench:      46,188 ns/iter (+/- 12,031)
