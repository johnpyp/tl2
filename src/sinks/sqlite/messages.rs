use anyhow::Result;
use sqlx::SqlitePool;

use crate::formats::orl::OrlLog;

pub async fn init_unified_messages_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
            PRAGMA journal_mode = OFF;
            PRAGMA cache_size = 1000000;

            PRAGMA locking_mode = EXCLUSIVE;
            PRAGMA synchronous = 0;
            "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS unified_messages (
            kind TEXT NOT NULL,
            id TEXT NOT NULL,
            timestamp INTEGER NOT NULL,

            username TEXT,
            channel_name TEXT,
            channel_id TEXT,
            twitch_id TEXT,
            text TEXT,

            PRIMARY KEY(kind, id)
        );

      "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn submit_orl_message_batch(pool: &SqlitePool, logs: Vec<OrlLog>) -> Result<()> {
    let mut tx = pool.begin().await?;

    for log in logs {
        let log = log.normalize();

        let id = log.get_id();
        let ts = log.get_unix_millis();
        let username = &log.username;
        let channel = &log.channel;
        let text = &log.text;

        sqlx::query(
            r#"
              INSERT OR REPLACE INTO unified_messages(
                  kind,
                  id,
                  timestamp,

                  username,
                  channel_name,
                  text
              )

              VALUES (?, ?, ?, ?, ?, ?);
            "#,
        )
        .bind("orl-log/1.0")
        .bind(id)
        .bind(ts)
        .bind(username)
        .bind(channel)
        .bind(text)
        .execute(&mut tx)
        .await?;
    }

    tx.commit().await?;

    Ok(())
}
