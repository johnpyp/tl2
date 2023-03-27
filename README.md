# TL2

Twitch Logger 2

## Prerequisites

1.68+ stable rust

## Usage

```bash
cargo build --release
mv ./target/release/tl2 .

# ...

./tl2 scrape

# Currently uses ClickhouseOrlMessage format defined in `src/adapters/clickhouse/messages_table.rs`
./tl2 jsonl-to-clickhouse ./json-logs --url 'http://localhost:8123'

# Currently uses the template format defined by `initialize_template` in `src/adapters/elasticsearch.rs`
./tl2 jsonl-to-elasticsearch ./json-logs --url 'http://localhost:9200' --index 'rustlesearch'

# Converts a "orl structured directory" to json logs format directory
./tl2 dir-to-jsonl ./orl-logs ./json-logs

# Reads json logs at max speed and prints out a "benchmark" :)
./tl2 jsonl-to-console ./json-logs
```

### License

MIT license
