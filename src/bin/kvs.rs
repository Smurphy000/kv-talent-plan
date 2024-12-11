use clap::{Parser, Subcommand};
use kvs::{CLIError, KVSError, KvStore};
use std::env;
use std::{io::Error, os, path::Path};

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Set { k: String, v: String },
    Get { k: String },
    Rm { k: String },
}
fn main() -> Result<(), KVSError> {
    let cli = Cli::parse();

    let log_path = env::current_dir()?;
    let p = Path::new(&log_path);
    let mut store = KvStore::open(p)?;

    match &cli.command {
        Some(Commands::Set { k, v }) => {
            store.set(k.to_string(), v.to_string())?;
            ()
        }
        Some(Commands::Get { k }) => {
            let v = store.get(k.to_string())?;
            match v {
                Some(v) => println!("{}", v),
                None => println!("Key not found"),
            }
            ()
        }
        Some(Commands::Rm { k }) => match store.remove(k.to_string()) {
            Ok(_) => (),
            Err(e) => {
                println!("Key not found");
                return Err(KVSError::DSError(e));
            }
        },
        _ => return Err(KVSError::CLIError(CLIError::NoCommand)),
    }
    // println!("{:?}", store);
    Ok(())
}
