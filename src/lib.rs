#[macro_use]
extern crate log;

use futures::future::join_all;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{mpsc, oneshot};

// Use Jemalloc only for musl-64 bits platforms
//#[cfg(all(target_env = "musl", target_pointer_width = "64"))]
//#[global_allocator]
//static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

type Frame = Vec<u8>;
type ReplySender = oneshot::Sender<Frame>;

#[derive(Debug)]
enum Message {
    Connection,
    Disconnection,
    Packet(Frame, ReplySender),
}

type ChannelRx = mpsc::Receiver<Message>;
type ChannelTx = mpsc::Sender<Message>;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, Error>;

type TcpReader = BufReader<tokio::net::tcp::OwnedReadHalf>;
type TcpWriter = tokio::net::tcp::OwnedWriteHalf;

fn frame_size(frame: &[u8]) -> Result<usize> {
    Ok(u16::from_be_bytes(frame[4..6].try_into()?) as usize)
}

fn split_connection(stream: TcpStream) -> (TcpReader, TcpWriter) {
    let (reader, writer) = stream.into_split();
    (BufReader::new(reader), writer)
}

async fn create_connection(
    url: &str,
    connect_delay: std::time::Duration,
) -> Result<(TcpReader, TcpWriter)> {
    let stream = TcpStream::connect(url).await?;
    stream.set_nodelay(true)?;
    if !connect_delay.is_zero() {
        debug!(
            "Waiting {:?} after connecting to backend {}",
            connect_delay, url
        );
        tokio::time::sleep(connect_delay).await;
    }
    Ok(split_connection(stream))
}

async fn read_frame(stream: &mut TcpReader) -> Result<Frame> {
    let mut buf = vec![0u8; 6];
    // Read header
    stream.read_exact(&mut buf).await?;
    // calculate payload size
    let total_size = 6 + frame_size(&buf)?;
    buf.resize(total_size, 0);
    stream.read_exact(&mut buf[6..total_size]).await?;
    Ok(buf)
}

#[derive(Debug, Deserialize)]
struct Listen {
    bind: String,
}

#[derive(Debug, Deserialize)]
struct Modbus {
    url: String,
    #[serde(default)]
    connect_delay_ms: u64,
}

struct Device {
    url: String,
    connect_delay: std::time::Duration,
    stream: Option<(TcpReader, TcpWriter)>,
}

impl Device {
    pub fn new(url: &str, connect_delay_ms: u64) -> Device {
        Device {
            url: url.to_string(),
            connect_delay: std::time::Duration::from_millis(connect_delay_ms),
            stream: None,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        match create_connection(&self.url, self.connect_delay).await {
            Ok(connection) => {
                info!(
                    "modbus connection to {} successful (connect_delay_ms = {})",
                    self.url,
                    self.connect_delay.as_millis()
                );
                debug!("Established backend TCP connection to {}", self.url);
                self.stream = Some(connection);
                Ok(())
            }
            Err(error) => {
                self.stream = None;
                warn!("modbus connection to {} error: {} ", self.url, error);
                Err(error)
            }
        }
    }

    fn disconnect(&mut self) {
        self.stream = None;
    }

    fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    async fn raw_write_read(&mut self, frame: &Frame) -> Result<Frame> {
        let (reader, writer) = self.stream.as_mut().ok_or("no modbus connection")?;
        trace!(
            "Writing {} bytes to backend {}: {:02X?}",
            frame.len(),
            self.url,
            frame
        );
        writer.write_all(&frame).await?;
        // No flush needed for TcpStream half
        let request_id = &frame[0..2];
        let timeout = std::time::Duration::from_secs(5);
        let reply = tokio::time::timeout(timeout, read_frame(reader)).await??;
        trace!(
            "Read {} bytes from backend {}: {:02X?}",
            reply.len(),
            self.url,
            reply
        );
        if &reply[0..2] != request_id {
            return Err(format!(
                "transaction ID mismatch: expected {:?}, got {:?}",
                request_id,
                &reply[0..2]
            )
            .into());
        }
        Ok(reply)
    }

    async fn write_read(&mut self, frame: &Frame) -> Result<Frame> {
        if self.is_connected() {
            let result = self.raw_write_read(&frame).await;
            match result {
                Ok(reply) => Ok(reply),
                Err(error) => {
                    warn!("modbus error: {}. Retrying...", error);
                    self.connect().await?;
                    self.raw_write_read(&frame).await
                }
            }
        } else {
            self.connect().await?;
            self.raw_write_read(&frame).await
        }
    }

    async fn handle_packet(&mut self, frame: Frame, channel: ReplySender) -> Result<()> {
        info!("modbus request to {}: {} bytes", self.url, frame.len());
        debug!("modbus request to {}: {:02X?}", self.url, &frame[..]);
        let reply = self.write_read(&frame).await?;
        info!("modbus reply from {}: {} bytes", self.url, reply.len());
        debug!("modbus reply from {}: {:02X?}", self.url, &reply[..]);
        let _ = channel.send(reply);
        Ok(())
    }

    async fn run(&mut self, channel: &mut ChannelRx) {
        let mut nb_clients = 0;

        while let Some(message) = channel.recv().await {
            match message {
                Message::Connection => {
                    nb_clients += 1;
                    info!("new client connection (active = {})", nb_clients);
                }
                Message::Disconnection => {
                    nb_clients -= 1;
                    info!("client disconnection (active = {})", nb_clients);
                    if nb_clients == 0 {
                        info!("disconnecting from modbus at {} (no clients)", self.url);
                        self.disconnect();
                    }
                }
                Message::Packet(frame, channel) => {
                    if let Err(e) = self.handle_packet(frame, channel).await {
                        error!("Backend error from {}: {}", self.url, e);
                        self.disconnect();
                    }
                }
            }
        }
    }

    async fn launch(url: &str, connect_delay_ms: u64, channel: &mut ChannelRx) {
        let mut modbus = Self::new(url, connect_delay_ms);
        modbus.run(channel).await;
    }
}

#[derive(Debug, Deserialize)]
struct Bridge {
    listen: Listen,
    modbus: Modbus,
}

impl Bridge {
    pub async fn run(&mut self) {
        let listener = TcpListener::bind(&self.listen.bind).await.unwrap();
        let modbus_url = self.modbus.url.clone();
        let connect_delay_ms = self.modbus.connect_delay_ms;
        let (tx, mut rx) = mpsc::channel::<Message>(32);
        tokio::spawn(async move {
            Device::launch(&modbus_url, connect_delay_ms, &mut rx).await;
        });
        info!(
            "Ready to accept requests on {} to {} (connect_delay_ms = {})",
            &self.listen.bind, &self.modbus.url, connect_delay_ms
        );
        loop {
            let (client, addr) = listener.accept().await.unwrap();
            info!("Accepted client connection from {}", addr);
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::handle_client(client, tx).await {
                    error!("Client error: {:?}", err);
                }
            });
        }
    }

    async fn handle_client(client: TcpStream, channel: ChannelTx) -> Result<()> {
        let peer_addr = client
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        debug!("Client {} stream setup", peer_addr);
        client.set_nodelay(true)?;
        channel.send(Message::Connection).await?;

        let result = Self::client_loop(client, &channel, &peer_addr).await;

        channel.send(Message::Disconnection).await?;
        info!("Client {} disconnected", peer_addr);

        result
    }

    async fn client_loop(client: TcpStream, channel: &ChannelTx, peer_addr: &str) -> Result<()> {
        let (mut reader, mut writer) = split_connection(client);
        while let Ok(buf) = read_frame(&mut reader).await {
            debug!(
                "Received request from client {}: {:02X?} ({} bytes)",
                peer_addr,
                &buf[..],
                buf.len()
            );
            let (tx, rx) = oneshot::channel();
            channel.send(Message::Packet(buf, tx)).await?;
            let reply = rx.await?;
            debug!(
                "Sending reply to client {}: {:02X?} ({} bytes)",
                peer_addr,
                &reply[..],
                reply.len()
            );
            writer.write_all(&reply).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct Server {
    devices: Vec<Bridge>,
}

impl Server {
    pub fn new(config_file: &str) -> std::result::Result<Self, config::ConfigError> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(config_file))
            .build()?;
        settings.try_deserialize()
    }

    pub async fn run(self) {
        let mut tasks = vec![];
        for mut bridge in self.devices {
            tasks.push(tokio::spawn(async move { bridge.run().await }));
        }

        #[cfg(unix)]
        let mut sigterm = signal(SignalKind::terminate()).unwrap();

        tokio::select! {
            _ = join_all(tasks) => debug!("All tasks finished"),
            _ = tokio::signal::ctrl_c() => debug!("Received Ctrl+C"),
            _ = async {
                #[cfg(unix)]
                {
                    sigterm.recv().await;
                    debug!("Received SIGTERM");
                }
                #[cfg(not(unix))]
                futures::future::pending::<()>().await
            } => {},
        }

        info!("Shutting down");
    }

    pub async fn launch(config_file: &str) -> std::result::Result<(), config::ConfigError> {
        Ok(Self::new(config_file)?.run().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_frame_size_logic() {
        let valid_header = [0x00, 0x01, 0x00, 0x00, 0x00, 0x05];
        assert_eq!(frame_size(&valid_header).unwrap(), 5);

        let large_header = [0x00, 0x01, 0x00, 0x00, 0xFF, 0xFF];
        assert_eq!(frame_size(&large_header).unwrap(), 65535);
    }

    #[tokio::test]
    async fn test_transaction_id_mismatch() {
        let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mock_listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _peer_addr) = mock_listener.accept().await.unwrap();
            let mut buf = vec![0u8; 12];
            let _ = stream.read_exact(&mut buf).await;

            // Send reply with a WRONG transaction ID (0x99, 0x99) instead of (0x00, 0x01)
            let wrong_reply: Frame = vec![
                0x99, 0x99, 0x00, 0x00, 0x00, 0x05, 0x01, 0x03, 0x02, 0x00, 0x00,
            ];
            let _ = stream.write_all(&wrong_reply).await;
        });

        let mut device = Device::new(&addr.to_string(), 0);
        device.connect().await.unwrap();

        // Request with transaction ID 0x00 0x01
        let request_frame: Frame = vec![
            0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];

        let result = device.raw_write_read(&request_frame).await;

        if result.is_err() {
            let err_msg = result.err().unwrap().to_string();
            assert!(
                err_msg.contains("transaction ID mismatch"),
                "Incorrect err: {}",
                err_msg
            );
        } else {
            panic!("Expected an error, but got success: {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_valid_transaction_id() {
        let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mock_listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = mock_listener.accept().await.unwrap();
            let mut buf = vec![0u8; 12];
            let _ = stream.read_exact(&mut buf).await;

            // Send reply with CORRECT transaction ID (0x00, 0x01)
            let valid_reply: Frame = vec![
                0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 0x01, 0x03, 0x02, 0x00, 0x00,
            ];
            let _ = stream.write_all(&valid_reply).await;
        });

        let mut device = Device::new(&addr.to_string(), 0);
        device.connect().await.unwrap();

        let request_frame: Frame = vec![
            0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];

        let result = device.raw_write_read(&request_frame).await;
        assert!(result.is_ok(), "Expected success, got {:?}", result);
        let reply = result.unwrap();
        assert_eq!(reply[0..2], request_frame[0..2]);
    }

    #[tokio::test]
    async fn test_backend_timeout() {
        let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mock_listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = mock_listener.accept().await.unwrap();
            let mut buf = vec![0u8; 12];
            let _ = stream.read_exact(&mut buf).await;

            // Hang indefinitely instead of replying
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        });

        let mut device = Device::new(&addr.to_string(), 0);
        device.connect().await.unwrap();

        let request_frame: Frame = vec![
            0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];

        let start_time = std::time::Instant::now();
        let result = device.raw_write_read(&request_frame).await;
        let elapsed = start_time.elapsed();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("deadline has elapsed") || err_msg.contains("timeout"),
            "Got msg: {}",
            err_msg
        );
        assert!(elapsed.as_secs() >= 4, "Should have taken 5s timeout");
    }

    #[tokio::test]
    async fn test_multiplex_concurrency() {
        let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mock_listener.local_addr().unwrap();

        // The Mock Server simulates the single persistent Modbus backend
        tokio::spawn(async move {
            let (mut stream, _) = mock_listener.accept().await.unwrap();
            // We expect two requests multiplexed onto this same connection
            for _ in 0..2 {
                let mut buf = vec![0u8; 12];
                // Read a complete 12-byte Modbus request
                let _ = stream.read_exact(&mut buf).await;
                // Simply echo it back (simulating a matched transaction ID)
                let _ = stream.write_all(&buf).await;
            }
        });

        // Launch the internal device channel multiplexer
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(32);
        tokio::spawn(async move {
            Device::launch(&addr.to_string(), 0, &mut rx).await;
        });

        // Client 1 sends a request with Transaction ID (0x00, 0x01)
        let tx1 = tx.clone();
        let client1 = tokio::spawn(async move {
            let request_frame: Frame = vec![
                0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
            ];
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            tx1.send(Message::Packet(request_frame, reply_tx))
                .await
                .unwrap();
            let reply = reply_rx.await.unwrap();
            assert_eq!(
                reply[0..2],
                [0x00, 0x01],
                "Client 1 got wrong Transaction ID"
            );
        });

        // Client 2 concurrently sends a request with Transaction ID (0x00, 0x02)
        let tx2 = tx.clone();
        let client2 = tokio::spawn(async move {
            let request_frame: Frame = vec![
                0x00, 0x02, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
            ];
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            tx2.send(Message::Packet(request_frame, reply_tx))
                .await
                .unwrap();
            let reply = reply_rx.await.unwrap();
            assert_eq!(
                reply[0..2],
                [0x00, 0x02],
                "Client 2 got wrong Transaction ID"
            );
        });

        // Join both client tasks; they should both smoothly succeed
        // through the exact same backend device connection
        tokio::try_join!(client1, client2).unwrap();
    }

    #[tokio::test]
    async fn test_connect_delay_waits_before_first_backend_request() {
        let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = mock_listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = mock_listener.accept().await.unwrap();
            let connected_at = std::time::Instant::now();
            let mut buf = vec![0u8; 12];
            let _ = stream.read_exact(&mut buf).await;
            assert!(
                connected_at.elapsed() >= std::time::Duration::from_millis(100),
                "backend request arrived before configured connect delay"
            );
            let valid_reply: Frame = vec![
                0x00, 0x01, 0x00, 0x00, 0x00, 0x05, 0x01, 0x03, 0x02, 0x00, 0x00,
            ];
            let _ = stream.write_all(&valid_reply).await;
        });

        let mut device = Device::new(&addr.to_string(), 100);
        device.connect().await.unwrap();

        let request_frame: Frame = vec![
            0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x01, 0x03, 0x00, 0x00, 0x00, 0x01,
        ];

        let reply = device.raw_write_read(&request_frame).await.unwrap();
        assert_eq!(reply[0..2], request_frame[0..2]);
    }
}
