use crate::connection::*;
use crate::errors;
use crate::serve::*;
use crate::settings::*;
use crate::status;
///
/// This module handles the necessary configuration to serve over TLS
///
use async_channel::Sender;
use async_tls::TlsAcceptor;
use rustls::server::ServerConfig;
use rustls::{Certificate, PrivateKey, RootCertStore};
use smol::io::BufReader;
use smol::net::TcpStream;
use tracing::log::*;

use std::path::Path;
use std::sync::Arc;

/// TlsServer is a syslog-over-TLS implementation, which will allow for receiving logs over a TLS
/// encrypted channel.
///
/// Currently client authentication is not supported
pub struct TlsServer {
    acceptor: TlsAcceptor,
}

impl TlsServer {
    pub fn new(state: &ServerState) -> Self {
        let config =
            load_tls_config(state).expect("Failed to generate the TLS ServerConfig properly");
        let acceptor = TlsAcceptor::from(Arc::new(config));
        TlsServer { acceptor }
    }
}

impl Server for TlsServer {
    fn bootstrap(&mut self, _state: &ServerState) -> Result<(), errors::HotdogError> {
        Ok(())
    }

    fn handle_connection(
        &self,
        stream: TcpStream,
        connection: Connection,
        stats: Sender<status::Statistic>,
    ) -> Result<(), std::io::Error> {
        debug!("Accepting from: {}", stream.peer_addr()?);

        // Calling `acceptor.accept` will start the TLS handshake
        let handshake = self.acceptor.accept(stream);

        smol::spawn(async move {
            // The handshake is a future we can await to get an encrypted
            // stream back.
            match handshake.await {
                Ok(tls_stream) => {
                    let reader = BufReader::new(tls_stream);

                    if let Err(e) = connection.read_logs(reader).await {
                        error!("Failure occurred while read_logs executed: {:?}", e);
                    }
                }
                Err(err) => {
                    error!("Unable to establish a TLS Stream for client! {:?}", err);
                }
            };

            let _ = stats.send((status::Stats::ConnectionCount, -1)).await;
        })
        .detach();
        Ok(())
    }
}

/// Generate the default ServerConfig needed for rustls to work properly in server mode
fn load_tls_config(state: &ServerState) -> std::io::Result<ServerConfig> {
    match &state.settings.global.listen.tls {
        TlsType::CertAndKey { cert, key, ca } => {
            let mut keys = load_keys(key.as_path())?;

            if keys.is_empty() {
                panic!("TLS key could not be properly loaded! This is fatal!");
            }

            if let Some(ca_path) = ca.as_ref() {
                panic!("Using a custom Certificate Authority is not currently supported!");
            } else {
                debug!("Loading the certificate and key into the ServerConfig: {cert:?}");
                let mut pemfile_reader =
                    std::io::BufReader::new(std::fs::File::open(cert.as_path())?);
                let certs: Vec<Certificate> = rustls_pemfile::certs(&mut pemfile_reader)
                    .expect("Failed to load certs")
                    .into_iter()
                    .map(Certificate)
                    .collect();
                Ok(ServerConfig::builder()
                    .with_safe_defaults()
                    .with_no_client_auth()
                    .with_single_cert(certs, keys.remove(0))
                    .expect("Not able to create the ServerConfig"))
            }
        }
        _ => {
            panic!("Attempted to load a TLS configuration despite TLS not being enabled");
        }
    }
}

/// Loads the keys file passed in, whether it is an RSA or PKCS8 formatted key
fn load_keys(path: &Path) -> std::io::Result<Vec<PrivateKey>> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut keys = rustls_pemfile::read_all(&mut reader)?;

    match keys.len() {
        1 => match keys.remove(0) {
            rustls_pemfile::Item::RSAKey(key) => Ok(vec![PrivateKey(key)]),
            rustls_pemfile::Item::PKCS8Key(key) => Ok(vec![PrivateKey(key)]),
            item => Err(std::io::Error::other(format!(
                "Failed to load keys properly found {item:?}"
            ))),
        },
        other => Err(std::io::Error::other(format!(
            "Failed to load keys properly, {other:?} found"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_keys_rsa() {
        let key_path = Path::new("./contrib/cert-key.pem");
        if let Ok(keys) = load_keys(key_path) {
            assert_eq!(1, keys.len());
        } else {
            panic!("Failed to find or load an RSA key");
        }
    }

    #[test]
    fn test_load_keys_pkcs8() {
        let key_path = Path::new("./contrib/pkcs8-key.pem");
        if let Ok(keys) = load_keys(key_path) {
            assert_eq!(1, keys.len());
        } else {
            panic!("Failed to find or load a PKCS8 key");
        }
    }
}
