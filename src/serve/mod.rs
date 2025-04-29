//!
//! The serve module is responsible for general syslog over TCP serving functionality
//!

use crate::connection::*;
use crate::errors;
use crate::settings::Settings;
use crate::sink::Message;
use crate::sink::Sink;
use crate::sink::kafka::Kafka;
use crate::sink::parquet::Parquet;
use crate::status;
use async_trait::async_trait;
use dipstick::InputQueueScope;
use dipstick::InputScope;
use smol::io::BufReader;
use smol::net::{SocketAddr, TcpListener, TcpStream};
use smol::stream::StreamExt;
use tracing::log::*;

use std::sync::Arc;

pub mod plain;
pub mod tls;

/// State entity which can be passed into connection handlers and callbacks
#[derive(Clone)]
pub struct ServerState {
    /// A reference to the global Settings object for all configuration information
    pub settings: Arc<Settings>,
    /// A Sender for sending statistics to the status handler
    pub stats: InputQueueScope,
}

// The Server trait describes the necessary functionality to implement a new hotdog backend server
// which can receive syslog messages
#[async_trait]
pub trait Server {
    /// Bootstrap can/should be overridden by implementations which need to perform some work prior
    /// to the creation of the TcpListener and the incoming connection loop
    fn bootstrap(&mut self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    /// Shutdown scan/should be overridden by implementations which need to perform some work after
    /// the termination of the connection accept loop
    fn shutdown(&self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    /// Handle a single connection
    ///
    /// The close_channel parameter must be a clone of our connection-tracking channel Sender
    fn handle_connection(
        &self,
        stream: TcpStream,
        connection: Connection,
    ) -> Result<(), std::io::Error> {
        debug!("Accepting from: {}", stream.peer_addr()?);
        let reader = BufReader::new(stream);

        smol::spawn(async move {
            if let Err(e) = connection.read_logs(reader).await {
                error!("Failure occurred while read_logs executed: {:?}", e);
            }
        })
        .detach();

        Ok(())
    }

    /// Accept connections on the addr
    async fn accept_loop(
        &mut self,
        addr: &str,
        state: ServerState,
    ) -> Result<(), errors::HotdogError> {
        let addr: SocketAddr = addr.parse().expect("Failed to parse the listen address");
        let mut sender = None;

        // If the Kafka sink is defined in the configuration, then spin up the configuration
        if let Some(kafka_conf) = &state.settings.global.kafka {
            info!("Configuring a Kafka sink with: {kafka_conf:?}");
            let mut kafka = Kafka::new(
                kafka_conf.clone(),
                &state.settings.schemas,
                state.stats.clone(),
            );

            kafka.bootstrap().await;
            sender = Some(kafka.get_sender());

            smol::spawn(async move {
                debug!("Starting Kafka loop");
                kafka.runloop().await;
            })
            .detach();
        }

        if let Some(parquet_conf) = &state.settings.global.parquet {
            info!("Configuring a Parquet sink with: {parquet_conf:?}");
            let pq = Parquet::new(
                parquet_conf.clone(),
                &state.settings.schemas,
                state.stats.clone(),
            );
            sender = Some(pq.get_sender());
            smol::spawn(async move {
                debug!("Starting Parquet loop");
                pq.runloop().await;
                debug!("Ending Parquet loop");
                std::process::exit(0);
            })
            .detach();
        }

        let sender = sender.expect("Failed to configure a sink properly!");
        use std::sync::atomic::{AtomicBool, Ordering};
        let should_exit = Arc::new(AtomicBool::new(false));
        let se = should_exit.clone();

        let ctrlc_tx = sender.clone();

        ctrlc::set_handler(move || {
            info!("Interrupt has been received! Attempting to flush");
            se.store(true, Ordering::SeqCst);
            let tx = ctrlc_tx.clone();
            smol::block_on(async move {
                tx.send(Message::Flush { should_exit: true })
                    .await
                    .expect("Failed to send flush command");
            });
        });

        self.bootstrap(&state)?;

        let listener = TcpListener::bind(addr).await?;
        let mut incoming = listener.incoming();
        let mut conn_count = 0;

        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            debug!("Accepting from: {}", stream.peer_addr()?);

            let connection =
                Connection::new(state.settings.clone(), sender.clone(), state.stats.clone());

            conn_count += 1;
            state
                .stats
                .gauge(status::Stats::ConnectionCount.into())
                .value(conn_count);

            if let Err(e) = self.handle_connection(stream, connection) {
                error!("Failed to handle_connection properly: {:?}", e);
            }
            conn_count -= 1;
            state
                .stats
                .gauge(status::Stats::ConnectionCount.into())
                .value(conn_count);

            if should_exit.load(Ordering::Relaxed) {
                debug!("Serve has been instructed to exit");
                break;
            }
        }

        self.shutdown(&state)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_placholder() {}
}
