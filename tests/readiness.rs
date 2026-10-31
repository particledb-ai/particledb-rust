use particledb_sdk::{Config, Error, ParticleDB, TlsConfig, Value};
use std::time::Duration;

fn config() -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: std::env::var("PDB_NATIVE_PORT")
            .expect("PDB_NATIVE_PORT")
            .parse()
            .unwrap(),
        prefer_unix_socket: false,
        pool_size: 1,
        ..Config::default()
    }
}

#[test]
fn credentials_are_redacted_and_invalid_config_is_rejected() {
    let c = Config {
        password: "private-credential".into(),
        ..Config::default()
    };
    assert!(!format!("{c:?}").contains("private-credential"));
    assert!(ParticleDB::connect(Config { pool_size: 0, ..c }).is_err());
}

#[test]
fn transactions_commit_rollback_and_refuse_a_swallowed_statement_error() {
    if std::env::var("PDB_NATIVE_PORT").is_err() {
        return;
    }
    let db = ParticleDB::connect(config()).unwrap();
    let table = format!("rust_readiness_{}", std::process::id());
    db.execute(
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"),
        &[],
    )
    .unwrap();
    db.transaction(|tx| {
        tx.execute(
            &format!("INSERT INTO {table} VALUES ($1, $2)"),
            &[Value::Int64(1), Value::String("first".into())],
        )?;
        Ok(())
    })
    .unwrap();
    let failed: Result<(), Error> = db.transaction(|tx| {
        tx.execute(
            &format!("INSERT INTO {table} VALUES (2, 'rolled back')"),
            &[],
        )?;
        Err(Error::Query("stop".into()))
    });
    assert!(failed.is_err());
    let failed = db.transaction(|tx| {
        let _ = tx.execute(&format!("INSERT INTO {table} VALUES (1, 'duplicate')"), &[]);
        Ok(())
    });
    assert!(failed.is_err());
    assert_eq!(
        db.query_one(&format!("SELECT count(*) AS n FROM {table}"), &[])
            .unwrap()
            .unwrap()
            .get_by_name::<i64>("n")
            .unwrap(),
        1
    );
    db.execute(&format!("DROP TABLE {table}"), &[]).unwrap();
}

#[test]
fn pool_size_limits_borrowers_and_a_timed_out_waiter_does_not_leak_a_slot() {
    if std::env::var("PDB_NATIVE_PORT").is_err() {
        return;
    }
    let db = std::sync::Arc::new(
        ParticleDB::connect(Config {
            connect_timeout: Duration::from_millis(100),
            ..config()
        })
        .unwrap(),
    );
    let (sent, received) = std::sync::mpsc::channel();
    let first = db.clone();
    let thread = std::thread::spawn(move || {
        first.transaction(|_| {
            sent.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(300));
            Ok(())
        })
    });
    received.recv().unwrap();
    assert!(db
        .query("SELECT 1", &[])
        .unwrap_err()
        .to_string()
        .contains("pool wait timed out"));
    thread.join().unwrap().unwrap();
    assert_eq!(db.query("SELECT 1", &[]).unwrap().rows.len(), 1);
}

#[test]
fn tls_verifies_the_server_and_refuses_wrong_hosts_or_plaintext() {
    let (Ok(port), Ok(ca)) = (std::env::var("PDB_TLS_PORT"), std::env::var("PDB_TLS_CA")) else {
        return;
    };
    let c = Config {
        host: "localhost".into(),
        port: port.parse().unwrap(),
        tls: Some(TlsConfig {
            root_cert: Some(ca.into()),
        }),
        ..Config::default()
    };
    let db = ParticleDB::connect(c.clone()).unwrap();
    assert_eq!(db.query("SELECT 42", &[]).unwrap().rows.len(), 1);
    assert!(ParticleDB::connect(Config {
        host: "127.0.0.1".into(),
        ..c.clone()
    })
    .is_err());
    assert!(ParticleDB::connect(Config {
        tls: Some(TlsConfig::default()),
        ..c
    })
    .is_err());
    if std::env::var("PDB_NATIVE_PORT").is_ok() {
        assert!(ParticleDB::connect(Config {
            tls: Some(TlsConfig::default()),
            ..config()
        })
        .is_err());
    }
}

#[test]
fn public_helpers_handle_values_and_create_real_pipeline_definitions() {
    if std::env::var("PDB_NATIVE_PORT").is_err() {
        return;
    }
    let db = ParticleDB::connect(config()).unwrap();
    let key = format!("rust:{}:'; SELECT 1; --", std::process::id());
    let value = "O'Reilly \\ 雪";
    db.kv().set(&key, value).unwrap();
    assert_eq!(db.kv().get(&key).unwrap().as_deref(), Some(value));
    assert!(db.kv().delete(&key).unwrap());
    assert_eq!(db.kv().get(&key).unwrap(), None);
    assert!(!db.kv().delete(&key).unwrap());
    let name = format!("rust_pipeline_{}", std::process::id());
    db.rag()
        .create_pipeline(&name, "documents", "embedding", "content", 5)
        .unwrap();
    db.execute(&format!("DROP RAG PIPELINE {name}"), &[])
        .unwrap();
    assert!(db
        .rag()
        .create_pipeline("bad; SELECT 1", "documents", "embedding", "content", 5)
        .is_err());
    let table = format!("rust_vector_{}", std::process::id());
    db.execute(
        &format!("CREATE TABLE {table} (id BIGINT, embedding VECTOR(2))"),
        &[],
    )
    .unwrap();
    db.execute(
        &format!("INSERT INTO {table} VALUES (1, VECTOR('[1,2]'))"),
        &[],
    )
    .unwrap();
    let rows = db
        .vector()
        .search(&table, "embedding", &[1.0, 2.0], 1, None)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].distance, 0.0);
    db.execute(&format!("DROP TABLE {table}"), &[]).unwrap();
}
