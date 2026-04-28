use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

use crate::metrics::StreamStats;

pub async fn serve_metrics(addr: String, stats: Arc<Mutex<StreamStats>>) -> Result<()> {
    let addr: SocketAddr = addr.parse()?;
    let listener = TcpListener::bind(addr).await?;
    serve_metrics_listener(listener, stats);
    Ok(())
}

fn serve_metrics_listener(listener: TcpListener, stats: Arc<Mutex<StreamStats>>) {
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let stats = stats.clone();
            tokio::spawn(async move {
                let mut buffer = [0_u8; 1024];
                let _ = socket.read(&mut buffer).await;
                let stats = stats.lock().await;
                let body = format!(
                    "# TYPE qwhyper_bytes_sent counter\nqwhyper_bytes_sent {}\n# TYPE qwhyper_records_sent counter\nqwhyper_records_sent {}\n# TYPE qwhyper_batches_sent counter\nqwhyper_batches_sent {}\n# TYPE qwhyper_retries counter\nqwhyper_retries {}\n# TYPE qwhyper_bytes_per_second gauge\nqwhyper_bytes_per_second {}\n",
                    stats.bytes_sent,
                    stats.records_sent,
                    stats.batches_sent,
                    stats.retries,
                    stats.bytes_per_sec()
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(), body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let stats = Arc::new(Mutex::new(StreamStats::new(100)));
        stats
            .lock()
            .await
            .record_batch(20, 2, 1, Duration::from_millis(5));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        serve_metrics_listener(listener, stats);

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("qwhyper_bytes_sent 20"));
        assert!(response.contains("qwhyper_records_sent 2"));
    }
}
