use std::convert::TryInto;

use criterion::{criterion_group, criterion_main, Criterion};
use mysql_time::MysqlTime;

criterion_group!(benches, serde);
criterion_main!(benches);

fn serde(c: &mut Criterion) {
    use bincode::Options;
    use noria_data::DataType;

    let mut group = c.benchmark_group("Serde");

    let tiny_string = "Tiny Text";
    let string = "This text is a big longer than TinyText";
    let long_string = string.repeat(4); // Four times as long as short string

    let tiny_text = DataType::TinyText(tiny_string.try_into().unwrap());
    let text = DataType::Text(string.into());
    let long_text = DataType::Text(long_string.as_str().into());
    let short_bytes = DataType::ByteArray(string.as_bytes().to_vec().into());
    let long_bytes = DataType::ByteArray(long_string.as_bytes().to_vec().into());

    let timestamp_tz = DataType::from(chrono::TimeZone::from_utc_datetime(
        &chrono::FixedOffset::west(18_000),
        &chrono::NaiveDateTime::from_timestamp(0, 42_000_000),
    ));
    let timestamp = DataType::Timestamp(chrono::NaiveDateTime::from_timestamp(0, 42_000_000));

    let time: DataType = MysqlTime::from_hmsus(false, 10, 30, 24, 100).into();

    let mut temp_storage = Vec::with_capacity(500);

    group.bench_function("Serialize TinyText", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &tiny_text)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Serialize short Text", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &text)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Serialize long Text", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &long_text)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Serialize short ByteArray", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &short_bytes)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Serialize long ByteArray", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &long_bytes)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Deserialize TinyText", |b| {
        let tt = bincode::options().serialize(&tiny_text).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&tt).unwrap())
    });

    group.bench_function("Deserialize short Text", |b| {
        let t = bincode::options().serialize(&text).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });

    group.bench_function("Deserialize long Text", |b| {
        let t = bincode::options().serialize(&long_text).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });

    group.bench_function("Deserialize short ByteArray", |b| {
        let t = bincode::options().serialize(&short_bytes).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });

    group.bench_function("Deserialize long ByteArray", |b| {
        let t = bincode::options().serialize(&long_bytes).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });

    group.bench_function("Serialize TimestampTz", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &timestamp_tz)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Deserialize TimestampTz", |b| {
        let t = bincode::options().serialize(&timestamp_tz).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });

    group.bench_function("Serialize Timestamp", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &timestamp)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Deserialize Timestamp", |b| {
        let t = bincode::options().serialize(&timestamp).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });

    group.bench_function("Serialize Time", |b| {
        b.iter(|| {
            bincode::options()
                .serialize_into(&mut temp_storage, &time)
                .unwrap();
            temp_storage.clear();
        })
    });

    group.bench_function("Deserialize Time", |b| {
        let t = bincode::options().serialize(&time).unwrap();
        b.iter(|| bincode::options().deserialize::<DataType>(&t).unwrap())
    });
}
