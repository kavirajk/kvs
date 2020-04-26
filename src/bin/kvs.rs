use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvError, KvStore, DEFAULT_LOG_NAME};
use std::error::Error;
use std::process::exit;

fn main() -> Result<(), KvError> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string ")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    let mut kv = KvStore::open(DEFAULT_LOG_NAME)?;

    match matches.subcommand() {
        ("get", Some(matches)) => {
            let mut values = matches.values_of("KEY").unwrap();
            let key = values.next().unwrap();

            match kv.get(key.to_owned())? {
                Some(v) => {
                    println!("{}", v);
                }
                None => println!("Key not found"),
            }
        }
        ("set", Some(matches)) => {
            let mut values = matches.values_of("KEY").unwrap();

            let key = values.next().unwrap();

            let mut values = matches.values_of("VALUE").unwrap();
            let val = values.next().unwrap();

            kv.set(key.to_owned(), val.to_owned())?;
        }
        ("rm", Some(matches)) => {
            let mut values = matches.values_of("KEY").unwrap();
            let key = values.next().unwrap();

            if let Err(e) = kv.remove(key.to_owned()) {
                println!("{}", e);
                exit(1);
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
