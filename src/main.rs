use std::path::PathBuf;

use env_logger::Env;
use log::error;
use log::info;
use run_scrape_ingester::run_ingester;

pub mod adapters;
pub mod alerts;
pub mod events;
pub mod formats;
pub mod run_scrape_ingester;
pub mod scrapers;
pub mod scripts;
pub mod settings;
pub mod sinks;
pub mod sources;
pub mod sqlite_pool;

use clap::Parser;
use clap::ValueHint;
use clap::{self};
use scripts::file_to_sqlite::dir_to_sqlite;

use crate::scripts::file_to_clickhouse::dir_to_clickhouse;
use crate::scripts::file_to_clickhouse::files_to_clickhouse;
use crate::scripts::file_to_elasticsearch::dir_to_elasticsearch;

#[derive(Parser, Debug)]
#[clap(name = "tl2")]
/// The twitch data stealing toolbox
enum Opt {
    /* /// fetch branches from remote repository
    Fetch {
        #[clap(long)]
        dry_run: bool,
        #[clap(long)]
        all: bool,
        #[clap(default_value = "origin")]
        repository: String,
    },
    #[clap(override_help = "add files to the staging area")]
    Add {
        #[clap(short)]
        interactive: bool,
        #[clap(short)]
        all: bool,
        files: Vec<String>,
    }, */
    /// Run twitch/dgg scraper and ingest based on config files
    Scrape {},
    /// Ingest structured directory with ORL-formatted files to clickhouse
    DirToClickhouse {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.txt(.gz)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,

        /// Clickhouse database url
        #[clap(short, long, default_value = "http://localhost:8123")]
        url: String,
    },
    /// Ingest ORL-formatted file(s) to clickhouse
    FileToClickhouse {
        /// Files with ORL-formatted logs, optionally ending in .gz for gzip compression
        #[clap(required = true, min_values = 1, value_hint = ValueHint::FilePath)]
        files: Vec<PathBuf>,

        /// Channel that these logs should be indexed as
        #[clap(short, long, required = true)]
        channel: String,

        /// Clickhouse database url
        #[clap(short, long, default_value = "http://localhost:8123")]
        url: String,
    },

    DirToElasticsearch {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.txt(.gz)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,

        /// Elasticsearch database url
        #[clap(short, long, required = true)]
        url: String,

        /// Elasticsearch index
        #[clap(short, long, required = true)]
        index: String,
    },

    DirToSqlite {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.txt(.gz)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,
    },
    DirToJsonl {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.txt(.gz)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,

        /// Output directory to store processed files
        #[clap(value_hint = ValueHint::DirPath)]
        output_directory: PathBuf,
    },

    JsonlToConsole {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.jsonl(.gz|.br)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,
    },
    JsonlToElasticsearch {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.jsonl(.gz|.br)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,

        /// Elasticsearch database url
        #[clap(short, long, required = true)]
        url: String,

        /// Elasticsearch index
        #[clap(short, long, required = true)]
        index: String,
    },
    JsonlToClickhouse {
        /// Directory with file structure: <root>/<Channel name>/<YYYY-MM-DD>.jsonl(.gz|.br)
        #[clap(value_hint = ValueHint::DirPath)]
        directory: PathBuf,

        /// Clickhouse database url
        #[clap(short, long, required = true)]
        url: String,
    },
}
#[tokio::main]
async fn main() {
    let cli_opts = Opt::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    match cli_opts {
        Opt::Scrape {} => {
            if let Err(e) = run_ingester().await {
                error!("{:?}", e);
            }
        }
        Opt::DirToClickhouse { directory, url } => {
            info!("Directory: {:?}", directory);
            info!("Clickhouse Url: {:?}", url);
            if let Err(e) = dir_to_clickhouse(directory, &url).await {
                error!("{:?}", e);
            }
        }
        Opt::FileToClickhouse {
            files,
            url,
            channel,
        } => {
            info!("Input Files: {:?}", files);
            info!("Clickhouse Url: {:?}", url);
            if let Err(e) = files_to_clickhouse(files, &channel, &url).await {
                error!("{:?}", e);
            }
        }
        Opt::DirToElasticsearch {
            directory,
            url,
            index,
        } => {
            info!("Directory: {:?}", directory);
            info!("Elasticsearch Url: {:?}", url);
            info!("Index: {:?}", index);

            if let Err(e) = dir_to_elasticsearch(directory, &url, &index).await {
                error!("{:?}", e);
            }
        }
        Opt::DirToSqlite { directory } => {
            info!("Directory: {:?}", directory);

            if let Err(e) = dir_to_sqlite(directory).await {
                error!("{:?}", e);
            }
        }
        Opt::DirToJsonl {
            directory,
            output_directory,
        } => {
            info!("Directory: {:?}", directory);
            info!("Output directory: {:?}", output_directory);

            if let Err(e) = scripts::dir_to_jsonl(directory, output_directory).await {
                error!("{:?}", e);
            }
        }
        Opt::JsonlToConsole { directory } => {
            info!("Directory: {:?}", directory);

            if let Err(e) = scripts::jsonl_to_console(directory).await {
                error!("{:?}", e);
            }
        }
        Opt::JsonlToElasticsearch {
            directory,
            url,
            index,
        } => {
            info!("Directory: {:?}", directory);
            info!("Elasticsearch Url: {:?}", url);
            info!("Elasticsearch Index: {:?}", index);

            if let Err(e) = scripts::jsonl_to_elasticsearch(directory, url, index).await {
                error!("{:?}", e);
            }
        }
        Opt::JsonlToClickhouse { directory, url } => {
            info!("Directory: {:?}", directory);
            info!("Clickhouse Url: {:?}", url);

            if let Err(e) = scripts::jsonl_to_clickhouse(directory, url).await {
                error!("{:?}", e);
            }
        }
    }
}
