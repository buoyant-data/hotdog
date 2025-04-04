//! The sink module contains the different sinks for hotdog
//!
//! This is not meant to be a robust rich system, like Vector, but just something simplistic to
//! serve syslog only

pub mod kafka;

///
/// The Sink trait is a simple interface for defining a thing that takes a bunch of rows and
/// outputs them into the appropriate thing.
///
/// When :hotdog: was first created, it could only spit rows into Apache Kafka. It has since
/// learned how to push data directly to storage as well, but to keep the majority of the serving
/// code the same, the Sink trait is needed.
///
#[async_trait::async_trait]
pub trait Sink: Default {
    type Config;

    /// Construct the Sink.
    ///
    /// This function should not do anything but initialize settings and variables
    fn new(config: Self::Config) -> Self;

    /// Bootstrap the sink
    ///
    /// This function is asynchronous and takes ownership of the Sink and then gives it back. It
    /// may modify the [Sink] during its execution if necessary
    async fn bootstrap(self) -> Self {
        self
    }

    /// Shutdown the sink
    ///
    /// This function may perform closing actions to flush buffers, etc on the [Sink] before it
    /// is to be cleaned up.
    async fn shutdown(self) -> Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestSink {
        config: Option<()>,
    }

    impl Sink for TestSink {
        type Config = Option<()>;

        fn new(config: Option<()>) -> Self {
            Self { config }
        }
    }

    #[test]
    fn test_sink() {
        let _sink = TestSink::default();
    }
}
