use anyhow::Result;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use tl2::sources::orl::line_parser::parse_message_line;
use tl2::sources::orl::line_parser::OrlLineMessage;
use tl2::sources::orl::parse_timestamp::parse_timestamp;
use tl2::sources::orl::parse_timestamp::parse_timestamp_slow;

pub fn parse_timestamp_benchmark(c: &mut Criterion) {
    let test_input = "2021-08-03 17:40:27.313 UTC";

    let mut group = c.benchmark_group("parse_timestamp");

    group.bench_with_input(
        BenchmarkId::new("Custom parsing", test_input),
        test_input,
        |b, i| b.iter(|| parse_timestamp(i)),
    );
    group.bench_with_input(
        BenchmarkId::new("Chrono parsing", test_input),
        test_input,
        |b, i| b.iter(|| parse_timestamp_slow(i)),
    );

    group.finish();
}

pub fn parse_message_line_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_message_line");

    let test_message_payload_1 = vec![
        "[2021-08-03 17:40:27.313 UTC] jOhNpYp: Example message",
        "[2021-08-03 17:40:27.313] jOhNpYp: Example message",
        "[2021-08-03 17:40:27] jOhNpYp: Example message",
        "[2021-08-03 17:40:27 UTC] jOhNpYp: Example message",
        "[2021-08-03 17:40:27 UTC]   test cat: Example message",
        "[2021-08-03 17:40:27 UTC]   tEst Cat  :   Example message   ",
        "[  2021-08-03 17:40:27 UTC  ]   TeSt Cat  :   Example message   ",
        "[2021-08-03 17:40:27 UTC] test cat: Example message\nfollowing message",
        "[0001-01-01 00:00:00 UTC] user: message",
        "[9999-12-31 23:59:59 UTC] user: message",
        "[2021-08-03 17:40:27 UTC] user: a",
        "[2021-08-03 17:40:27 UTC] user:",
        "[2021-08-03 17:40:27 UTC]user: message",
    ];
    group.throughput(criterion::Throughput::Bytes(
        test_message_payload_1
            .iter()
            .map(|x| x.len())
            .sum::<usize>() as u64,
    ));
    group.bench_with_input(
        BenchmarkId::new("Custom splitting parser", "Test message payload 1"),
        &test_message_payload_1,
        |b, i| {
            b.iter(|| {
                i.iter()
                    .map(|s| parse_message_line(s))
                    .collect::<Vec<Result<OrlLineMessage>>>()
            })
        },
    );
    // group.bench_with_input(BenchmarkId::new("Chrono parsing", i), i, |b, i| {
    //     b.iter(|| parse_timestamp_slow(i))
    // });

    group.finish();
}

criterion_group!(
    benches,
    parse_timestamp_benchmark,
    parse_message_line_benchmark
);
criterion_main!(benches);
