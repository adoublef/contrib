use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::{spawn, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let stream = stream::iter(0..10);
    let (tx, rx) = mpsc::channel(1);
    let worker_a = spawn({
        async move {
            let tx = tx.clone();
            stream
                .map(Ok::<_, anyhow::Error>)
                .try_for_each_concurrent(1, |v| {
                    let tx = tx.clone();
                    async move {
                        tx.send(v).await.unwrap();
                        Ok::<_, anyhow::Error>(())
                    }
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });
    let stream = ReceiverStream::new(rx);
    let (tx, mut rx) = mpsc::channel(1);
    let worker_b = spawn({
        // dont want to move other resources
        async move {
            let tx = tx.clone();
            stream
                .map(Ok::<_, anyhow::Error>)
                .try_for_each_concurrent(1, |v| {
                    let tx = tx.clone();
                    async move {
                        // for v in 0..v {
                        let mut it = 0..v;
                        while let Some(v) = it.next() {
                            tx.send((v, v)).await.unwrap();
                        }
                        Ok::<_, anyhow::Error>(())
                    }
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });

    while let Some((a, b)) = rx.recv().await {
        println!("{a}{b}");
    }
    worker_a.await??;
    worker_b.await??;
    Ok(())
}
