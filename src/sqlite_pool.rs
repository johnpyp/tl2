use std::path::Path;

use log::{info, LevelFilter};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    ConnectOptions, SqlitePool,
};
use tokio::fs;

pub async fn create_sqlite(db_path: &str) -> Result<SqlitePool, sqlx::Error> {
    info!("Initializing sqlite with path: {}", db_path);
    let path_parent = Path::new(db_path).parent();
    if let Some(p) = path_parent {
        fs::create_dir_all(p).await?;
    }
    let connect_options = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .log_statements(LevelFilter::Trace)
        .to_owned();
    let pool = SqlitePoolOptions::new()
        .connect_with(connect_options)
        .await?;

    Ok(pool)
}
