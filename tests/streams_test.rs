#![cfg(all(feature = "client", feature = "server"))]

use anyhow::{Ok, Result};
use common::{PING, PONG};
use rand::Rng;
use rathole::{AsyncStream, ClientServiceEvent, Config, ServerServiceEvent, ServiceType};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{broadcast, mpsc},
    time,
};
use tracing::{debug, info, instrument};
use tracing_subscriber::EnvFilter;

#[allow(dead_code)]
mod common;

const ECHO_SERVER_ADDR: &str = "127.0.0.1:8080";
const PINGPONG_SERVER_ADDR: &str = "127.0.0.1:8081";
const ECHO_SERVER_ADDR_EXPOSED: &str = "127.0.0.1:2334";
const PINGPONG_SERVER_ADDR_EXPOSED: &str = "127.0.0.1:2335";
const HITTER_NUM: usize = 4;

#[derive(Clone, Copy, Debug)]
enum Type {
    Tcp,
    Udp,
}

fn init() {
    let level = "info";
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from(level)),
        )
        .try_init();
}

#[tokio::test]
async fn tcp() -> Result<()> {
    init();

    // The client serves the echo and pingpong services from their visitor streams

    test("tests/for_tcp/tcp_transport.toml", Type::Tcp).await?;

    #[cfg(any(
         // FIXME: Self-signed certificate on macOS nativetls requires manual interference.
         all(target_os = "macos", feature = "rustls"),
         // On other OS accept run with either
         all(not(target_os = "macos"), any(feature = "native-tls", feature = "rustls")),
     ))]
    test("tests/for_tcp/tls_transport.toml", Type::Tcp).await?;

    #[cfg(feature = "noise")]
    test("tests/for_tcp/noise_transport.toml", Type::Tcp).await?;

    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_tcp/websocket_transport.toml", Type::Tcp).await?;

    #[cfg(not(target_os = "macos"))]
    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_tcp/websocket_tls_transport.toml", Type::Tcp).await?;

    Ok(())
}

#[tokio::test]
async fn udp() -> Result<()> {
    init();

    // Spawn a echo server
    tokio::spawn(async move {
        if let Err(e) = common::udp::echo_server(ECHO_SERVER_ADDR).await {
            panic!("Failed to run the echo server for testing: {:?}", e);
        }
    });

    // Spawn a pingpong server
    tokio::spawn(async move {
        if let Err(e) = common::udp::pingpong_server(PINGPONG_SERVER_ADDR).await {
            panic!("Failed to run the pingpong server for testing: {:?}", e);
        }
    });

    test("tests/for_udp/tcp_transport.toml", Type::Udp).await?;

    #[cfg(any(
         // FIXME: Self-signed certificate on macOS nativetls requires manual interference.
         all(target_os = "macos", feature = "rustls"),
         // On other OS accept run with either
         all(not(target_os = "macos"), any(feature = "native-tls", feature = "rustls")),
     ))]
    test("tests/for_udp/tls_transport.toml", Type::Udp).await?;

    #[cfg(feature = "noise")]
    test("tests/for_udp/noise_transport.toml", Type::Udp).await?;

    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_udp/websocket_transport.toml", Type::Udp).await?;

    #[cfg(not(target_os = "macos"))]
    #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
    test("tests/for_udp/websocket_tls_transport.toml", Type::Udp).await?;

    Ok(())
}

#[instrument]
async fn test(config_path: &'static str, t: Type) -> Result<()> {
    if cfg!(not(all(feature = "client", feature = "server"))) {
        // Skip the test if the client or the server is not enabled
        return Ok(());
    }

    let (client_shutdown_tx, client_shutdown_rx) = broadcast::channel(1);
    let (server_shutdown_tx, server_shutdown_rx) = broadcast::channel(1);

    // Start the client
    info!("start the client");
    let client = tokio::spawn(async move {
        run_rathole_client(config_path, client_shutdown_rx)
            .await
            .unwrap();
    });

    // Sleep for 1 second. Expect the client keep retrying to reach the server
    time::sleep(Duration::from_secs(1)).await;

    // Start the server
    info!("start the server");
    let server = tokio::spawn(async move {
        run_rathole_server(config_path, server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await; // Wait for the client to retry

    info!("echo");
    echo_hitter(ECHO_SERVER_ADDR_EXPOSED, t).await.unwrap();
    info!("pingpong");
    pingpong_hitter(PINGPONG_SERVER_ADDR_EXPOSED, t)
        .await
        .unwrap();

    // Simulate the client crash and restart
    info!("shutdown the client");
    client_shutdown_tx.send(true)?;
    tokio::join!(client).0?;

    info!("restart the client");
    let client_shutdown_rx = client_shutdown_tx.subscribe();
    let client = tokio::spawn(async move {
        run_rathole_client(config_path, client_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_secs(1)).await; // Wait for the client to start

    info!("echo");
    echo_hitter(ECHO_SERVER_ADDR_EXPOSED, t).await.unwrap();
    info!("pingpong");
    pingpong_hitter(PINGPONG_SERVER_ADDR_EXPOSED, t)
        .await
        .unwrap();

    // Simulate the server crash and restart
    info!("shutdown the server");
    server_shutdown_tx.send(true)?;
    tokio::join!(server).0?;

    info!("restart the server");
    let server_shutdown_rx = server_shutdown_tx.subscribe();
    let server = tokio::spawn(async move {
        run_rathole_server(config_path, server_shutdown_rx)
            .await
            .unwrap();
    });
    time::sleep(Duration::from_millis(2500)).await; // Wait for the client to retry

    // Simulate heavy load
    info!("lots of echo and pingpong");

    let mut v = Vec::new();

    for _ in 0..HITTER_NUM / 2 {
        v.push(tokio::spawn(async move {
            echo_hitter(ECHO_SERVER_ADDR_EXPOSED, t).await.unwrap();
        }));

        v.push(tokio::spawn(async move {
            pingpong_hitter(PINGPONG_SERVER_ADDR_EXPOSED, t)
                .await
                .unwrap();
        }));
    }

    for h in v {
        assert!(tokio::join!(h).0.is_ok());
    }

    // Shutdown
    info!("shutdown the server and the client");
    server_shutdown_tx.send(true)?;
    client_shutdown_tx.send(true)?;

    let (server, client) = tokio::join!(server, client);
    server?;
    client?;

    Ok(())
}

// Run a client that serves the visitor streams of each TCP service as tests/common
// serves the service. Each service that starts stops by the end, and only TCP
// services have visitor streams
async fn run_rathole_client(
    config_path: &str,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let config = Config::from_file(Path::new(config_path)).await?;
    let (client_event_tx, mut client_event_rx) = mpsc::unbounded_channel();
    let (_, config_change_rx) = mpsc::channel(1);
    let client = rathole::run_client_with_visitor_queue(
        config,
        shutdown_rx,
        config_change_rx,
        client_event_tx,
    );

    let mut up_service_names = Vec::new();
    let client_event_handler = async {
        while let Some(e) = client_event_rx.recv().await {
            match e {
                ClientServiceEvent::TcpStarted {
                    config,
                    mut visitor_stream_rx,
                } => {
                    assert_eq!(config.service_type, ServiceType::Tcp);
                    assert!(
                        !up_service_names.contains(&config.name),
                        "{} started twice",
                        config.name
                    );
                    up_service_names.push(config.name.clone());
                    tokio::spawn(async move {
                        while let Some(visitor_stream) = visitor_stream_rx.recv().await {
                            serve(&config.name, visitor_stream);
                        }
                    });
                }
                ClientServiceEvent::UdpStarted { config } => {
                    assert_eq!(config.service_type, ServiceType::Udp);
                    assert!(
                        !up_service_names.contains(&config.name),
                        "{} started twice",
                        config.name
                    );
                    up_service_names.push(config.name.clone());
                }
                ClientServiceEvent::Stopped {
                    name: downing_service_name,
                } => {
                    assert!(
                        up_service_names.contains(&downing_service_name),
                        "{} stopped but never started",
                        downing_service_name
                    );
                    up_service_names.retain(|n| *n != downing_service_name);
                }
            }
        }
    };

    let (ret, _) = tokio::join!(client, client_event_handler);
    ret?;
    assert!(
        up_service_names.is_empty(),
        "{:?} never stopped",
        up_service_names
    );
    Ok(())
}

// Serve `visitor_stream` as tests/common serves the TCP service `name`
fn serve(name: &str, visitor_stream: Box<dyn AsyncStream>) {
    match name {
        "echo" => tokio::spawn(async move {
            let (mut rd, mut wr) = tokio::io::split(visitor_stream);
            let _ = tokio::io::copy(&mut rd, &mut wr).await;
        }),
        "pingpong" => tokio::spawn(async move {
            let mut visitor_stream = visitor_stream;
            let mut buf = [0u8; PING.len()];
            while visitor_stream.read_exact(&mut buf).await.is_ok() {
                assert_eq!(buf, PING.as_bytes());
                if visitor_stream.write_all(PONG.as_bytes()).await.is_err() {
                    break;
                }
            }
        }),
        _ => panic!("Unexpected service {}", name),
    };
}

// Run a server that accepts the visitors of each TCP service at its
// `bind_addr` itself, and sends them to the client of the service while it is
// connected. Each service that connects disconnects by the end, and only TCP
// services take visitors
async fn run_rathole_server(
    config_path: &str,
    shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    let config = Config::from_file(Path::new(config_path)).await?;
    let (server_event_tx, mut server_event_rx) = mpsc::unbounded_channel();
    let (_, config_change_rx) = mpsc::channel(1);

    // The visitor queue of each connected service, or `None` for a UDP service
    let up_visitor_txs: Arc<Mutex<HashMap<String, Option<mpsc::Sender<Box<dyn AsyncStream>>>>>> =
        Arc::default();
    let mut listeners = Vec::new();
    for service in config.server.as_ref().unwrap().services.values() {
        if service.service_type != ServiceType::Tcp {
            continue;
        }
        let tcp_listener = TcpListener::bind(&service.bind_addr).await?;
        let (name, up) = (service.name.clone(), up_visitor_txs.clone());
        listeners.push(tokio::spawn(async move {
            while let std::result::Result::Ok((conn, _)) = tcp_listener.accept().await {
                let visitor_tx = up.lock().unwrap().get(&name).cloned().flatten();
                if let Some(visitor_tx) = visitor_tx {
                    let _ = visitor_tx.send(Box::new(conn)).await;
                }
            }
        }));
    }

    let server = rathole::run_server_with_visitor_queue(
        config,
        shutdown_rx,
        config_change_rx,
        server_event_tx,
    );
    let track = async {
        while let Some(e) = server_event_rx.recv().await {
            match e {
                ServerServiceEvent::TcpConnected { config, visitor_tx } => {
                    assert_eq!(config.service_type, ServiceType::Tcp);
                    let old = up_visitor_txs
                        .lock()
                        .unwrap()
                        .insert(config.name.clone(), Some(visitor_tx));
                    assert!(old.is_none(), "{} connected twice", config.name);
                }
                ServerServiceEvent::UdpConnected { config } => {
                    assert_eq!(config.service_type, ServiceType::Udp);
                    let old = up_visitor_txs
                        .lock()
                        .unwrap()
                        .insert(config.name.clone(), None);
                    assert!(old.is_none(), "{} connected twice", config.name);
                }
                ServerServiceEvent::Disconnected { name } => {
                    let old = up_visitor_txs.lock().unwrap().remove(&name);
                    assert!(old.is_some(), "{} disconnected but never connected", name);
                }
            }
        }
    };

    let (ret, _) = tokio::join!(server, track);
    for l in listeners {
        l.abort();
        let _ = l.await;
    }
    ret?;
    let up = up_visitor_txs.lock().unwrap();
    assert!(up.is_empty(), "{:?} never disconnected", up.keys());
    Ok(())
}

async fn echo_hitter(addr: &'static str, t: Type) -> Result<()> {
    match t {
        Type::Tcp => tcp_echo_hitter(addr).await,
        Type::Udp => udp_echo_hitter(addr).await,
    }
}

async fn pingpong_hitter(addr: &'static str, t: Type) -> Result<()> {
    match t {
        Type::Tcp => tcp_pingpong_hitter(addr).await,
        Type::Udp => udp_pingpong_hitter(addr).await,
    }
}

async fn tcp_echo_hitter(addr: &'static str) -> Result<()> {
    let mut conn = TcpStream::connect(addr).await?;

    let mut wr = [0u8; 1024];
    let mut rd = [0u8; 1024];
    for _ in 0..100 {
        rand::thread_rng().fill(&mut wr);
        conn.write_all(&wr).await?;
        conn.read_exact(&mut rd).await?;
        assert_eq!(wr, rd);
    }

    Ok(())
}

async fn udp_echo_hitter(addr: &'static str) -> Result<()> {
    let conn = UdpSocket::bind("127.0.0.1:0").await?;
    conn.connect(addr).await?;

    let mut wr = [0u8; 128];
    let mut rd = [0u8; 128];
    for _ in 0..3 {
        rand::thread_rng().fill(&mut wr);

        conn.send(&wr).await?;
        debug!("send");

        conn.recv(&mut rd).await?;
        debug!("recv");

        assert_eq!(wr, rd);
    }
    Ok(())
}

async fn tcp_pingpong_hitter(addr: &'static str) -> Result<()> {
    let mut conn = TcpStream::connect(addr).await?;

    let wr = PING.as_bytes();
    let mut rd = [0u8; PONG.len()];

    for _ in 0..100 {
        conn.write_all(wr).await?;
        conn.read_exact(&mut rd).await?;
        assert_eq!(rd, PONG.as_bytes());
    }

    Ok(())
}

async fn udp_pingpong_hitter(addr: &'static str) -> Result<()> {
    let conn = UdpSocket::bind("127.0.0.1:0").await?;
    conn.connect(&addr).await?;

    let wr = PING.as_bytes();
    let mut rd = [0u8; PONG.len()];

    for _ in 0..3 {
        conn.send(wr).await?;
        debug!("ping");

        conn.recv(&mut rd).await?;
        debug!("pong");

        assert_eq!(rd, PONG.as_bytes());
    }

    Ok(())
}
