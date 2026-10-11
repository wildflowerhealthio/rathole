#![cfg(all(feature = "client", feature = "server"))]

use anyhow::Result;
use rathole::{Config, ControlChannelError, ControlChannelState};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing_subscriber::EnvFilter;

// Long enough for a control channel to connect, fail and be reported
const STATE_TIMEOUT: Duration = Duration::from_secs(5);

fn init() {
    let level = "info";
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::from(level)),
        )
        .try_init();
}

#[tokio::test]
async fn server_going_away_disconnects() -> Result<()> {
    init();
    let server_addr = free_addr().await?;
    let (server_shutdown_tx, server) = run_server(&server_addr, "echo", "token").await?;
    let (_client_shutdown_tx, mut state_rx, _client) =
        run_client(&server_addr, "echo", "token").await?;

    let state = next_state(&mut state_rx).await;
    assert!(
        matches!(&state, ControlChannelState::Connected { name } if name == "echo"),
        "{:?}",
        state
    );

    server_shutdown_tx.send(true)?;
    server.await??;

    let state = next_state(&mut state_rx).await;
    assert!(
        matches!(
            &state,
            ControlChannelState::Disconnected {
                name,
                error: ControlChannelError::Failed(_),
                ..
            } if name == "echo"
        ),
        "{:?}",
        state
    );
    Ok(())
}

#[tokio::test]
async fn wrong_token_is_auth_failed() -> Result<()> {
    init();
    let server_addr = free_addr().await?;
    let (_server_shutdown_tx, _server) = run_server(&server_addr, "echo", "token").await?;
    let (_client_shutdown_tx, mut state_rx, _client) =
        run_client(&server_addr, "echo", "wrong_token").await?;

    let state = next_state(&mut state_rx).await;
    assert!(
        matches!(
            &state,
            ControlChannelState::Disconnected {
                name,
                error: ControlChannelError::AuthFailed,
                ..
            } if name == "echo"
        ),
        "{:?}",
        state
    );
    Ok(())
}

#[tokio::test]
async fn unknown_service_is_service_not_exist() -> Result<()> {
    init();
    let server_addr = free_addr().await?;
    let (_server_shutdown_tx, _server) = run_server(&server_addr, "other", "token").await?;
    let (_client_shutdown_tx, mut state_rx, _client) =
        run_client(&server_addr, "echo", "token").await?;

    let state = next_state(&mut state_rx).await;
    assert!(
        matches!(
            &state,
            ControlChannelState::Disconnected {
                name,
                error: ControlChannelError::ServiceNotExist,
                ..
            } if name == "echo"
        ),
        "{:?}",
        state
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_during_backoff_stops_retrying() -> Result<()> {
    init();

    // A server that hangs up on each control channel, counting them
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?.to_string();
    let dials = Arc::new(AtomicUsize::new(0));
    let counted_dials = dials.clone();
    tokio::spawn(async move {
        while let Ok((conn, _)) = listener.accept().await {
            counted_dials.fetch_add(1, Ordering::SeqCst);
            drop(conn);
        }
    });

    let (client_shutdown_tx, mut state_rx, client) =
        run_client(&server_addr, "echo", "token").await?;

    let state = next_state(&mut state_rx).await;
    let retry_in = match state {
        ControlChannelState::Disconnected {
            error: ControlChannelError::Failed(_),
            retry_in,
            ..
        } => retry_in,
        state => panic!("{:?}", state),
    };

    // Shut down as soon as the control channel is reported disconnected
    client_shutdown_tx.send(true)?;
    client.await??;

    // The states end once the control channel stops, well before it would retry
    let end = time::timeout(retry_in / 2, state_rx.recv()).await;
    assert!(
        matches!(end, Ok(None)),
        "the control channel did not stop during the backoff"
    );

    // And no further dial is made after the backoff would have passed
    time::sleep(retry_in * 2).await;
    assert_eq!(dials.load(Ordering::SeqCst), 1);
    Ok(())
}

// An address nothing is listening on
async fn free_addr() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    Ok(listener.local_addr()?.to_string())
}

// Run a server at `addr` with the single TCP service `name`
async fn run_server(
    addr: &str,
    name: &str,
    token: &str,
) -> Result<(broadcast::Sender<bool>, JoinHandle<Result<()>>)> {
    let config: Config = format!(
        r#"
        [server]
        bind_addr = "{addr}"

        [server.services.{name}]
        token = "{token}"
        bind_addr = "{bind_addr}"
        "#,
        bind_addr = free_addr().await?,
    )
    .parse()?;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (_, config_change_rx) = mpsc::channel(1);
    let server =
        tokio::spawn(
            async move { rathole::run_server(config, shutdown_rx, config_change_rx).await },
        );

    // Wait for the server to listen, so that the client's first dial reaches it
    while TcpStream::connect(addr).await.is_err() {
        time::sleep(Duration::from_millis(10)).await;
    }
    Ok((shutdown_tx, server))
}

// Run a client of the single TCP service `name` on the server at `addr`, with
// the state of its control channel
async fn run_client(
    addr: &str,
    name: &str,
    token: &str,
) -> Result<(
    broadcast::Sender<bool>,
    mpsc::UnboundedReceiver<ControlChannelState>,
    JoinHandle<Result<()>>,
)> {
    let config: Config = format!(
        r#"
        [client]
        remote_addr = "{addr}"

        [client.services.{name}]
        token = "{token}"
        local_addr = "127.0.0.1:1"
        "#,
    )
    .parse()?;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (_, config_change_rx) = mpsc::channel(1);
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (state_tx, state_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move { while event_rx.recv().await.is_some() {} });
    let client = tokio::spawn(async move {
        rathole::run_client_with_visitor_queue(
            config,
            shutdown_rx,
            config_change_rx,
            event_tx,
            state_tx,
        )
        .await
    });
    Ok((shutdown_tx, state_rx, client))
}

async fn next_state(
    state_rx: &mut mpsc::UnboundedReceiver<ControlChannelState>,
) -> ControlChannelState {
    time::timeout(STATE_TIMEOUT, state_rx.recv())
        .await
        .expect("no state in time")
        .expect("the states ended")
}
