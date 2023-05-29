use criterion::{black_box, criterion_group, criterion_main, Criterion};

async fn async_benchmark(iterations: usize, data: Vec<i64>) {
    let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel::<Vec<i64>>(1);

    let mut handles = Vec::new();
    for _ in 0..100 {
        let mut broadcast_rx = broadcast_tx.subscribe();
        let handle = tokio::spawn(async move {
            let mut sum = 0;
            while let Ok(data) = broadcast_rx.recv().await {
                data.iter().for_each(|x| sum += x);
            }
            sum
        });
        handles.push(handle);
    }

    let broadcaster_handle = tokio::spawn(async move {
        std::iter::repeat(data).take(iterations).for_each(|data| {
            broadcast_tx.send(data).unwrap();
        });
    });

    broadcaster_handle.await.unwrap();
    for handle in handles {
        handle.await.unwrap();
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("channel", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                async_benchmark(
                    black_box(1000),
                    black_box(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                )
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
