//!
//! The Parquet module contains the parquet sink which is mostly intended to be used with S3
//! storage backends
//!

use super::{Message, Sink};

use arrow_json::reader::ReaderBuilder;
use async_channel::{Receiver, Sender, bounded};
use async_compat::Compat;
use dipstick::InputQueueScope;
use object_store::ObjectStore;
use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use smol::stream::StreamExt;
use tracing::log::*;
use tracing::{Level, span};
use url::Url;
use uuid::Uuid;

use std::collections::HashMap;
use std::io::{Cursor, Seek, Write};
use std::sync::Arc;
use std::time::Instant;

/// Alias for convenience in refering to reference counted [ObjectStore]
type ObjectStoreRef = Arc<dyn ObjectStore>;

/// Parquet sink which handles creating parquet files from buffers and writing them into the
/// storage layer
#[derive(Clone)]
pub struct Parquet {
    /// Configuration from the hotdog.yml
    config: Config,
    /// Underlying object store
    store: ObjectStoreRef,
    /// Receiver side of the channel for this sink
    rx: Receiver<Message>,
    /// Producer side of the channel for this sink
    tx: Sender<Message>,
}

#[async_trait::async_trait]
impl Sink for Parquet {
    type Config = Config;

    fn new(config: Self::Config, _stats: InputQueueScope) -> Self {
        let (tx, rx) = bounded(1024);
        // [object_store] largely expects environment variables to be all lowercased for
        // consideration as options
        let opts: HashMap<String, String> = HashMap::from_iter(
            std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v))
        );
        let (store, _path) = object_store::parse_url_opts(&config.url, opts)
            .expect("Failed to parse the Parquet sink URL");
        let store = Arc::new(store);
        Parquet {
            config,
            tx,
            rx,
            store,
        }
    }

    fn get_sender(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn runloop(&self) {
        // Converting this at the beginning of the function to ensure that it doesn't need to be
        // done in the loop and if the configuration is invalid then the sink fails as early as
        // possible
        let flush_ms: u128 = self
            .config
            .flush_ms
            .try_into()
            .expect("Failed to convert the flush_ms to a 128 bit integer");

        debug!("Entering the Parquet sink runloop");
        smol::block_on(Compat::new(async {
            info!("Listing the bucket as a sanity check: {}", self.config.url);
            let mut list_stream = self.store.list(None);

            // Print a line about each object
            if let Some(meta) = list_stream.next().await.transpose().unwrap() {
                debug!("Name: {}, size: {}", meta.location, meta.size);
            }
            debug!("Finished listing the bucket");
        }));

        let mut buffer: HashMap<String, Vec<String>> = HashMap::default();
        let mut bufsizes: HashMap<String, usize> = HashMap::default();
        let mut since_last_flush = Instant::now();

        loop {
            if let Ok(msg) = self.rx.recv().await {
                debug!("Buffering this message for Parquet output: {msg:?}");
                let _span = span!(Level::INFO, "Parquet sink recv");

                if !buffer.contains_key(&msg.destination) {
                    buffer.insert(msg.destination.clone(), vec![]);
                    bufsizes.insert(msg.destination.clone(), 0);
                }

                if let Some(queue) = buffer.get_mut(&msg.destination) {
                    let bufsize = bufsizes
                        .get_mut(&msg.destination)
                        .expect("Failed to get the bufsizes somehow");
                    (*bufsize) += msg.payload.len();
                    debug!(
                        "enqueing into `{}` (bytes: {bufsize}): {:?}",
                        &msg.destination, &msg.payload
                    );
                    queue.push(msg.payload);

                    if (since_last_flush.elapsed().as_millis() > flush_ms)
                        || (*bufsize > self.config.buffer)
                    {
                        debug!(
                            "Reached the threshold to flush bytes for `{}`",
                            &msg.destination
                        );
                        if let Some(buf) = buffer.remove(&msg.destination) {
                            let _flush_span = span!(Level::INFO, "Parquet flush");
                            flush_to_parquet(self.store.clone(), &msg.destination, buf);
                            since_last_flush = Instant::now();
                        }
                    }
                }
            }
        }
    }
}

/// Write the given buffer to a new `.parquet` file in the [ObjectStore]
fn flush_to_parquet(store: ObjectStoreRef, destination: &str, buffer: Vec<String>) {
    if buffer.is_empty() {
        warn!("Attempted to flush_to_parquet with an empty buffer");
        return;
    }
    info!(
        "Flushing buffer with {} messages to {destination}",
        buffer.len()
    );

    //let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    //let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    let mut cursor: Cursor<&str> = Cursor::new(&buffer[0]);
    //let mut reader = BufReader::new(GzDecoder::new(&file));
    let (inferred_schema, _read) = arrow_json::reader::infer_json_schema(&mut cursor, None)
        .expect("Failed to process a JSON payload");
    debug!("inferred_schema! {inferred_schema:?}");
    // TODO: This hould be easier, hopefully without the bytes conversion for decoding.
    // The strings must come in fully formed as lines so the inferred_schema can work above
    let mut buf_read = Cursor::new(Vec::new());

    for line in buffer {
        buf_read
            .write_all(line.as_bytes())
            .expect("Failed to write all bytes into the buffer");
    }

    // Rewind for the reader
    let _ = buf_read.rewind();

    let schema = Arc::new(inferred_schema);
    let mut reader = ReaderBuilder::new(schema.clone())
        .build(buf_read)
        .expect("Failed to build the JSON decoder");

    let output =
        object_store::path::Path::from(format!("{destination}/{}.parquet", Uuid::new_v4()));

    let object_writer = ParquetObjectWriter::new(store.clone(), output.clone());
    let mut writer = AsyncArrowWriter::try_new(object_writer, schema.clone(), None)
        .expect("Failed to build AsyncArrowWriter");

    smol::block_on(Compat::new(async {
        while let Some(Ok(batch)) = reader.next() {
            writer.write(&batch).await.expect("Failed to write a batch");
        }
        let file_result = writer.close().await.expect("Failed to close the writer");
        info!("Flushed {} rows to storage", file_result.num_rows);
    }));
}

/// Configuration for [Parquet] sink
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default = "parquet_url_default")]
    /// Expected to be an S3 compatible URL
    pub url: Url,
    /// Minimum number of bytes to buffer into each parquet file
    #[serde(default = "parquet_buffer_default")]
    pub buffer: usize,
    /// Duration in milliseconds before a flush to storage should happen
    #[serde(default = "parquet_flush_default")]
    pub flush_ms: usize,
}

/// Retrieves a URL from the environment for parquet if no [Url] has been specified
fn parquet_url_default() -> Url {
    Url::parse(&std::env::var("S3_OUTPUT_URL").expect(
        "There is no url: defined for the parquet sink and no S3_OUTPUT_URL in the environment!",
    ))
    .expect("The S3_OUTPUT_URL could not be parsed as a valid URL")
}

/// Default number of log lines per parquet file
fn parquet_buffer_default() -> usize {
    1_024 * 1_024 * 100
}

/// Default milliseconds before a Parquet sink flush
fn parquet_flush_default() -> usize {
    1000 * 10
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
