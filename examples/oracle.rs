use std::net::SocketAddr;

use axum::{Router, response::IntoResponse, routing::get};
use bb8_oracle::OracleConnectionManager;
use serde::{Deserialize, Serialize};
use time::Duration;
use tokio::{signal, task::AbortHandle};
use tower_sessions::{Expiry, Session, SessionManagerLayer, session_store::ExpiredDeletion};
use tower_sessions_stores_oracle::OracleStore;

const COUNTER_KEY: &str = "counter";

#[derive(Serialize, Deserialize, Default)]
struct Counter(usize);

async fn handler(session: Session) -> impl IntoResponse {
    let counter: Counter = session.get(COUNTER_KEY).await.unwrap().unwrap_or_default();
    session.insert(COUNTER_KEY, counter.0 + 1).await.unwrap();
    format!("Current count: {}", counter.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::option_env!("ORACLE_URL").expect("Missing ORACLE_URL.");
    let database_user = std::option_env!("ORACLE_USER").expect("Missing ORACLE_USER.");
    let database_password = std::option_env!("ORACLE_PASSWORD").expect("Missing ORACLE_PASSWORD.");

    println!("Connecting to Oracle database at: {}", database_url);
    let manager = OracleConnectionManager::new(database_user, database_password, database_url);

    let pool = bb8::Pool::builder()
        .max_size(8)
        .build(manager)
        .await
        .unwrap();

    let session_store = OracleStore::new(pool).with_table_name("sessions")?;
    session_store.migrate().await?;
    println!("Session store migrated successfully.");

    let deletion_task = tokio::task::spawn(
        session_store
            .clone()
            .continuously_delete_expired(tokio::time::Duration::from_secs(60)),
    );

    let session_layer = SessionManagerLayer::new(session_store)
        .with_secure(false)
        .with_expiry(Expiry::OnInactivity(Duration::seconds(10)));

    let app = Router::new().route("/", get(handler)).layer(session_layer);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    // Ensure we use a shutdown signal to abort the deletion task.
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal(deletion_task.abort_handle()))
        .await?;

    deletion_task.await??;

    Ok(())
}

async fn shutdown_signal(deletion_task_abort_handle: AbortHandle) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { deletion_task_abort_handle.abort() },
        _ = terminate => { deletion_task_abort_handle.abort() },
    }
}
