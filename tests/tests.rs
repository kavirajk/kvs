use assert_cmd::prelude::*;
use kvs::KvStore;
use predicates::str::contains;
use std::process::Command;

#[test]
fn cli_no_args() {
    Command::cargo_bin("kvs").unwrap().assert().failure();
}

#[test]
fn cli_version() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn cli_get() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key"])
        .assert()
        .failure()
        .stderr(contains("unimplemented"));
}

#[test]
fn cli_set() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key", "value"])
        .assert()
        .failure()
        .stderr(contains("unimplemented"));
}

#[test]
fn cli_rm() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key"])
        .assert()
        .failure()
        .stderr(contains("unimplemented"));
}

#[test]
fn cli_invalid_get() {
    // empty
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    // two args for get
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "k1", "k2"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    // empty
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    // one args for set
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "k1"])
        .assert()
        .failure();

    // three args for set
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "k1", "k2", "k3"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    // empty
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    // two args for rm
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "k2", "k3"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["unknown"])
        .assert()
        .failure();
}

// KvSore tests

#[test]
fn get_stored_value() {
    let mut kv = KvStore::new();

    kv.set("k1".to_owned(), "v1".to_owned());
    kv.set("k2".to_owned(), "v2".to_owned());

    assert_eq!(kv.get("k2".to_owned()), Some("v2".to_string()));
    assert_eq!(kv.get("k1".to_owned()), Some("v1".to_string()));
}

#[test]
fn overwrite_value() {
    let mut kv = KvStore::default();

    kv.set("k1".to_owned(), "v1".to_owned());

    assert_eq!(kv.get("k1".to_owned()), Some("v1".to_string()));

    kv.set("k1".to_owned(), "v2".to_owned());

    assert_eq!(kv.get("k1".to_owned()), Some("v2".to_string()));
}

#[test]
fn test_nonexistent_key() {
    let kv = KvStore::new();

    assert_eq!(kv.get("k1".to_owned()), None);
}

#[test]
fn remove_key() {
    let mut kv = KvStore::new();

    kv.set("k".to_string(), "v".to_string());
    kv.rm("k".to_string());

    assert_eq!(kv.get("k".to_owned()), None);
}
