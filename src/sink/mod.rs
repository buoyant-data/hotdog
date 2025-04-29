//! The sink module contains the different sinks for hotdog
//!
//! This is not meant to be a robust rich system, like Vector, but just something simplistic to
//! serve syslog only

use async_channel::Sender;
use dipstick::InputQueueScope;

pub mod kafka;
pub mod parquet;

///
/// The Sink trait is a simple interface for defining a thing that takes a bunch of rows and
/// outputs them into the appropriate thing.
///
/// When :hotdog: was first created, it could only spit rows into Apache Kafka. It has since
/// learned how to push data directly to storage as well, but to keep the majority of the serving
/// code the same, the Sink trait is needed.
///
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    type Config;

    /// Construct the Sink.
    ///
    /// This function should not do anything but initialize settings and variables
    fn new(
        config: Self::Config,
        schemas: &[crate::settings::Schema],
        stats: InputQueueScope,
    ) -> Self;

    /// Bootstrap the sink
    ///
    /// This function is asynchronous and takes ownership of the Sink and then gives it back. It
    /// may modify the [Sink] during its execution if necessary
    ///
    /// This must be mutable to allow implementers of this trait update any internal data as part
    /// of a two-stage initialization (i.e. connection) process
    async fn bootstrap(&mut self) {}

    /// Return a [Sender] which is capable of communicating with the [Sink]
    fn get_sender(&self) -> Sender<Message>;

    /// Runloop which should be spawned into its own task for the sink to perform its consumption
    /// duties.
    ///
    /// This function should be spun into its own task and panic if anything goes wrong.
    async fn runloop(&self);
}

/// The [Message] struct is used for bringing messages between the receivers and the sinks that
/// will ultimately output them.
///
/// THe `destination` may interpreted differently depending on the [Sink]!
#[derive(Clone, Debug)]
pub enum Message {
    Data {
        destination: String,
        payload: String,
    },
    Flush {
        should_exit: bool,
    },
}

impl Message {
    pub fn new(destination: String, payload: String) -> Self {
        Message::Data {
            destination,
            payload,
        }
    }

    pub fn flush() -> Self {
        Message::Flush { should_exit: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestSink {
        config: Option<()>,
    }

    #[async_trait::async_trait]
    impl Sink for TestSink {
        type Config = Option<()>;

        fn new(
            config: Option<()>,
            _schemas: &[crate::settings::Schema],
            _stats: InputQueueScope,
        ) -> Self {
            Self { config }
        }

        fn get_sender(&self) -> Sender<Message> {
            let (tx, _rx) = async_channel::bounded(1);
            tx
        }

        async fn runloop(&self) {
            unreachable!("This should never be invoked in tests");
        }
    }

    #[test]
    fn test_sink() {
        let _sink = TestSink::default();
    }
}
