use clap::{Arg, Parser, Subcommand};
use kvs::KvStore;

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
fn main() {
    let cli = Cli::parse();

    let mut store = KvStore::new();

    match &cli.command {
        Some(Commands::Set { k, v }) => {
            store.set(k.to_string(), v.to_string());
        }
        Some(Commands::Get { k }) => {
            store.get(k.to_string());
            ()
        }
        Some(Commands::Rm { k }) => {
            store.remove(k.to_string());
            ()
        }
        _ => {}
    }
}
