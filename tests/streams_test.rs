#![cfg(all(feature = "client", feature = "server"))]

use anyhow::{Context, Result};
use rathole::{ClientEvent, Config, ConfigChange, ServerEvent, ServerServiceChange};
use std::future::Future;
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    time,
};

const TIMEOUT: Duration = Duration::from_secs(10);

// An address on loopback with a port that is free, for now
async fn free_addr() -> Result<String> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    Ok(l.local_addr()?.to_string())
}

// A server with the TCP service `echo` exposed at `echo_addr`, and the UDP
// service `dns`
fn server_config(bind_addr: &str, echo_addr: &str, dns_addr: &str) -> Result<Config> {
    format!(
        r#"
        [server]
        bind_addr = "{bind_addr}"

        [server.services.echo]
        bind_addr = "{echo_addr}"
        token = "echo_token"

        [server.services.dns]
        type = "udp"
        bind_addr = "{dns_addr}"
        token = "dns_token"
    "#
    )
    .parse()
}

// A client of the TCP service `echo`, served at `echo_addr`, and the UDP
// service `dns`
fn client_config(remote_addr: &str, echo_addr: &str) -> Result<Config> {
    format!(
        r#"
        [client]
        remote_addr = "{remote_addr}"

        [client.services.echo]
        local_addr = "{echo_addr}"
        token = "echo_token"

        [client.services.dns]
        type = "udp"
        local_addr = "127.0.0.1:53"
        token = "dns_token"
    "#
    )
    .parse()
}

// Echo everything read from `stream`
fn spawn_echo<S: AsyncRead + AsyncWrite + Send + 'static>(stream: S) {
    tokio::spawn(async move {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let _ = tokio::io::copy(&mut rd, &mut wr).await;
    });
}

// Send `ping` over `stream` and expect it back
async fn ping<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> Result<()> {
    stream.write_all(b"ping").await?;
    let mut rd = [0u8; 4];
    time::timeout(TIMEOUT, stream.read_exact(&mut rd))
        .await
        .context("Not echoed")??;
    assert_eq!(&rd, b"ping");
    Ok(())
}

// Visit `addr` until it echoes, as it is not listened at until the client
// connects
async fn ping_addr(addr: &str) -> Result<()> {
    time::timeout(TIMEOUT, async {
        loop {
            if let Ok(mut conn) = TcpStream::connect(addr).await {
                if ping(&mut conn).await.is_ok() {
                    return;
                }
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("The visitor was never echoed")
}

async fn recv<T>(events: &mut mpsc::UnboundedReceiver<T>) -> Result<T> {
    time::timeout(TIMEOUT, events.recv())
        .await
        .context("No event")?
        .context("The events ended")
}

// Assert no event comes within a while
async fn assert_no_event<T: std::fmt::Debug>(events: &mut mpsc::UnboundedReceiver<T>) {
    if let Ok(Some(e)) = time::timeout(Duration::from_millis(500), events.recv()).await {
        panic!("Unexpected event {:?}", e);
    }
}

// Run `f` with its own shutdown channel and no config changes
fn spawn_instance<F, Fut>(f: F) -> (broadcast::Sender<bool>, tokio::task::JoinHandle<Result<()>>)
where
    F: FnOnce(broadcast::Receiver<bool>, mpsc::Receiver<ConfigChange>) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (update_tx, update_rx) = mpsc::channel(1);
    let task = f(shutdown_rx, update_rx);
    (
        shutdown_tx,
        tokio::spawn(async move {
            let _update_tx = update_tx;
            task.await
        }),
    )
}

#[tokio::test]
async fn client_streams() -> Result<()> {
    let (bind_addr, echo_addr, dns_addr) =
        (free_addr().await?, free_addr().await?, free_addr().await?);
    let (server_shutdown_tx, server) = spawn_instance(|s, u| {
        rathole::run_server(
            server_config(&bind_addr, &echo_addr, &dns_addr).unwrap(),
            s,
            u,
        )
    });

    // `local_addr` is not connected to
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let config = client_config(&bind_addr, "127.0.0.1:1")?;
    let (client_shutdown_tx, client) =
        spawn_instance(|s, u| rathole::run_client_streams(config, s, u, events_tx));

    // Only the TCP service is reported
    let mut streams = match recv(&mut events).await? {
        ClientEvent::ServiceUp { config, streams } => {
            assert_eq!(config.name, "echo");
            streams
        }
        e => panic!("Unexpected event {:?}", e),
    };
    tokio::spawn(async move {
        while let Some(stream) = streams.recv().await {
            spawn_echo(stream);
        }
    });

    // A visitor at the server reaches the stream
    ping_addr(&echo_addr).await?;
    assert_no_event(&mut events).await;

    client_shutdown_tx.send(true)?;
    client.await??;
    match recv(&mut events).await? {
        ClientEvent::ServiceDown { name } => assert_eq!(name, "echo"),
        e => panic!("Unexpected event {:?}", e),
    }
    assert!(events.recv().await.is_none());

    server_shutdown_tx.send(true)?;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn local_addr_forwarding() -> Result<()> {
    let (bind_addr, echo_addr, dns_addr) =
        (free_addr().await?, free_addr().await?, free_addr().await?);

    let local = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = local.local_addr()?.to_string();
    tokio::spawn(async move {
        while let Ok((conn, _)) = local.accept().await {
            spawn_echo(conn);
        }
    });

    let (server_shutdown_tx, server) = spawn_instance(|s, u| {
        rathole::run_server(
            server_config(&bind_addr, &echo_addr, &dns_addr).unwrap(),
            s,
            u,
        )
    });
    let config = client_config(&bind_addr, &local_addr)?;
    let (client_shutdown_tx, client) = spawn_instance(|s, u| rathole::run_client(config, s, u));

    ping_addr(&echo_addr).await?;
    // Several visitors at once
    let mut visitors = Vec::new();
    for _ in 0..4 {
        visitors.push(TcpStream::connect(&echo_addr).await?);
    }
    for v in &mut visitors {
        ping(v).await?;
    }

    client_shutdown_tx.send(true)?;
    server_shutdown_tx.send(true)?;
    client.await??;
    server.await??;
    Ok(())
}

// Run a client of `remote_addr` that echoes every data channel of `echo`
fn spawn_echo_client(
    remote_addr: &str,
) -> Result<(broadcast::Sender<bool>, tokio::task::JoinHandle<Result<()>>)> {
    let config = client_config(remote_addr, "127.0.0.1:1")?;
    let (events_tx, mut events) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(e) = events.recv().await {
            if let ClientEvent::ServiceUp { mut streams, .. } = e {
                tokio::spawn(async move {
                    while let Some(stream) = streams.recv().await {
                        spawn_echo(stream);
                    }
                });
            }
        }
    });
    Ok(spawn_instance(|s, u| {
        rathole::run_client_streams(config, s, u, events_tx)
    }))
}

// Expect `ServiceUp` of `echo`, returning its visitor sender
async fn recv_echo_up(
    events: &mut mpsc::UnboundedReceiver<ServerEvent>,
) -> Result<rathole::VisitorSender> {
    match recv(events).await? {
        ServerEvent::ServiceUp { config, visitors } => {
            assert_eq!(config.name, "echo");
            Ok(visitors)
        }
        e => panic!("Unexpected event {:?}", e),
    }
}

async fn recv_echo_down(events: &mut mpsc::UnboundedReceiver<ServerEvent>) -> Result<()> {
    match recv(events).await? {
        ServerEvent::ServiceDown { name } => assert_eq!(name, "echo"),
        e => panic!("Unexpected event {:?}", e),
    }
    Ok(())
}

// Send a visitor to the client and expect it to be echoed
async fn ping_visitors(visitors: &rathole::VisitorSender) -> Result<()> {
    let (mut visitor, stream) = tokio::io::duplex(1024);
    visitors
        .send(Box::new(stream))
        .await
        .map_err(|_| anyhow::anyhow!("The visitor was not taken"))?;
    ping(&mut visitor).await
}

// Wait until the control channel behind `visitors` is gone
async fn wait_closed(visitors: &rathole::VisitorSender) -> Result<()> {
    time::timeout(TIMEOUT, async {
        while !visitors.is_closed() {
            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("The visitor sender was never closed")?;
    let (_visitor, stream) = tokio::io::duplex(1024);
    assert!(visitors.send(Box::new(stream)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn server_streams() -> Result<()> {
    let (bind_addr, echo_addr, dns_addr) =
        (free_addr().await?, free_addr().await?, free_addr().await?);
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (update_tx, update_rx) = mpsc::channel(1);
    let server = tokio::spawn(rathole::run_server_streams(
        server_config(&bind_addr, &echo_addr, &dns_addr)?,
        shutdown_rx,
        update_rx,
        events_tx,
    ));

    // Nothing is up until the client connects
    assert_no_event(&mut events).await;

    let (client_shutdown_tx, client) = spawn_echo_client(&bind_addr)?;
    let visitors = recv_echo_up(&mut events).await?;
    ping_visitors(&visitors).await?;
    // `bind_addr` is not listened at
    assert!(TcpStream::connect(&echo_addr).await.is_err());

    // The client reconnects. The old control channel is dropped before the new
    // one is reported
    client_shutdown_tx.send(true)?;
    client.await??;
    let (client_shutdown_tx, client) = spawn_echo_client(&bind_addr)?;
    recv_echo_down(&mut events).await?;
    wait_closed(&visitors).await?;
    let visitors = recv_echo_up(&mut events).await?;
    ping_visitors(&visitors).await?;

    // Removing the service drops its control channel
    update_tx
        .send(ConfigChange::ServerChange(ServerServiceChange::Delete(
            "echo".to_string(),
        )))
        .await?;
    recv_echo_down(&mut events).await?;
    wait_closed(&visitors).await?;

    // The UDP service is never reported
    assert_no_event(&mut events).await;

    client_shutdown_tx.send(true)?;
    shutdown_tx.send(true)?;
    client.await??;
    server.await??;
    Ok(())
}
