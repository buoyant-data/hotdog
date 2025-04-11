//!
//! The Parquet module contains the parquet sink which is mostly intended to be used with S3
//! storage backends
//!

use super::{Message, Sink};
use crate::status::Statistic;
use async_channel::{Receiver, Sender, bounded};

/// Parquet sink which handles creating parquet files from buffers and writing them into the
/// storage layer
#[derive(Debug, Clone)]
pub struct Parquet {
    config: Config,
    stats: Sender<Statistic>,
    rx: Receiver<Message>,
    tx: Sender<Message>,
}

#[async_trait::async_trait]
impl Sink for Parquet {
    type Config = Config;

    fn new(config: Self::Config, stats: Sender<Statistic>) -> Self {
        let (tx, rx) = bounded(100);
        Parquet {
            config,
            tx,
            rx,
            stats,
        }
    }

    fn get_sender(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn runloop(&self) {
        unimplemented!("Yet to be built!");
    }
}

/// Configuration for [Parquet] sink
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Config {
    /// Expected to be an S3 compatible URL
    pub url: url::Url,
    /// Minimum number of bytes to buffer into each parquet file
    #[serde(default = "parquet_buffer_default")]
    pub buffer: usize,
    /// Duration in milliseconds before a flush to storage should happen
    #[serde(default = "parquet_flush_default")]
    pub flush_ms: usize,
}

/// Default number of log lines per parquet file
fn parquet_buffer_default() -> usize {
    1_024 * 1_024 * 100
}

/// Default [Duration] before a Parquet sink flush
fn parquet_flush_default() -> usize {
    120
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        assert!(0 < parquet_buffer_default());
        assert!(0 < parquet_flush_default());
    }
}
