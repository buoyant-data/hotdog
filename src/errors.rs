//! This module only contains the expected errors emitted from Hotdog

#[derive(Debug)]
pub enum HotdogError {
    IOError { err: std::io::Error },
    KafkaConnectError,
}

impl std::convert::From<std::io::Error> for HotdogError {
    fn from(err: std::io::Error) -> HotdogError {
        HotdogError::IOError { err }
    }
}
