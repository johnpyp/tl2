use std::path::PathBuf;

use env_logger::Env;
use log::{error, info};
use run_scrape_ingester::run_ingester;

pub mod adapters;
pub mod alerts;
pub mod events;
pub mod orl_file_parser;
pub mod orl_line_parser;
pub mod run_scrape_ingester;
pub mod scrapers;
pub mod scripts;
pub mod settings;
pub mod sqlite_pool;

use clap::{self, Parser, ValueHint};

use crate::scripts::{
    file_to_clickhouse::{dir_to_clickhouse, files_to_clickhouse},
    file_to_elasticsearch::dir_to_elasticsearch,
};

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
    }
}
