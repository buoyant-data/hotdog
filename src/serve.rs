use crate::connection::*;
use crate::errors;
use crate::settings::Settings;
use crate::sink::kafka::Kafka;
use crate::status;
/**
 * The serve module is responsible for general syslog over TCP serving functionality
 */
use async_channel::Sender;
use async_trait::async_trait;
use smol::io::BufReader;
use smol::net::{SocketAddr, TcpListener, TcpStream};
use smol::stream::StreamExt;
use tracing::log::*;

use std::sync::Arc;

pub struct ServerState {
    /**
     * A reference to the global Settings object for all configuration information
     */
    pub settings: Arc<Settings>,
    /**
     * A Sender for sending statistics to the status handler
     */
    pub stats: Sender<status::Statistic>,
}

/**
 * The Server trait describes the necessary functionality to implement a new hotdog backend server
 * which can receive syslog messages
 */
#[async_trait]
pub trait Server {
    /**
     * Bootstrap can/should be overridden by implementations which need to perform some work prior
     * to the creation of the TcpListener and the incoming connection loop
     */
    fn bootstrap(&mut self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    /**
     * Shutdown scan/should be overridden by implementations which need to perform some work after
     * the termination of the connection accept loop
     */
    fn shutdown(&self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    /**
     * Handle a single connection
     *
     * The close_channel parameter must be a clone of our connection-tracking channel Sender
     */
    fn handle_connection(
        &self,
        stream: TcpStream,
        connection: Connection,
        stats: Sender<status::Statistic>,
    ) -> Result<(), std::io::Error> {
        debug!("Accepting from: {}", stream.peer_addr()?);
        let reader = BufReader::new(stream);

        smol::spawn(async move {
            if let Err(e) = connection.read_logs(reader).await {
                error!("Failure occurred while read_logs executed: {:?}", e);
            }

            let _ = stats.send((status::Stats::ConnectionCount, -1)).await;
        })
        .detach();

        Ok(())
    }

    /**
     * Accept connections on the addr
     */
    async fn accept_loop(
        &mut self,
        addr: &str,
        state: ServerState,
    ) -> Result<(), errors::HotdogError> {
        let addr: SocketAddr = addr.parse().expect("Failed to parse the listen address");
        let mut kafka = Kafka::new(state.settings.global.kafka.buffer, state.stats.clone());

        if !kafka.connect(
            &state.settings.global.kafka.conf,
            Some(state.settings.global.kafka.timeout_ms),
        ) {
            error!("Cannot start hotdog without a workable broker connection");
            return Err(errors::HotdogError::KafkaConnectError);
        }

        let sender = kafka.get_sender();

        let _task = smol::spawn(async move {
            debug!("Starting Kafka sendloop");
            kafka.sendloop().await;
        });

        self.bootstrap(&state)?;

        let listener = TcpListener::bind(addr).await?;
        let mut incoming = listener.incoming();

        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            debug!("Accepting from: {}", stream.peer_addr()?);

            let _ = state.stats.send((status::Stats::ConnectionCount, 1)).await;

            let connection =
                Connection::new(state.settings.clone(), sender.clone(), state.stats.clone());

            if let Err(e) = self.handle_connection(stream, connection, state.stats.clone()) {
                error!("Failed to handle_connection properly: {:?}", e);
            }
        }

        self.shutdown(&state)?;

        Ok(())
    }
}
