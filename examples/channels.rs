//! A small pipeline: several producer tasks feed a bounded `mpsc`
//! channel, a consumer task sums everything, and a `oneshot` carries
//! the final total back out.

use rusty_tokio::sync::{mpsc, oneshot};
use rusty_tokio::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    let total = rt.block_on(async {
        let (tx, mut rx) = mpsc::channel::<u64>(8);
        let (done_tx, done_rx) = oneshot::channel();

        let mut producers = Vec::new();
        for worker in 0..4u64 {
            let tx = tx.clone();
            producers.push(rusty_tokio::spawn(async move {
                for i in 0..25u64 {
                    tx.send(worker * 100 + i).await.unwrap();
                }
            }));
        }
        drop(tx); // so the consumer's `recv()` sees the channel close

        rusty_tokio::spawn(async move {
            let mut sum = 0u64;
            while let Some(v) = rx.recv().await {
                sum += v;
            }
            done_tx.send(sum).unwrap();
        });

        for p in producers {
            p.await.unwrap();
        }
        done_rx.await.unwrap()
    });
    println!("total: {total}");
}
