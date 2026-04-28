use bytes::{BufMut, Bytes, BytesMut};

use crate::reader::Record;

#[derive(Debug, Clone)]
pub struct BatcherConfig {
    pub max_batch_bytes: usize,
    pub max_batch_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRecordSpan {
    pub body_start: usize,
    pub body_end: usize,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    pub body: Bytes,
    pub record_count: usize,
    pub byte_count: usize,
    pub start_offset: u64,
    pub end_offset: u64,
    pub spans: Vec<BatchRecordSpan>,
}

impl Batch {
    pub fn split_half(&self) -> Option<(Batch, Batch)> {
        if self.spans.len() < 2 {
            return None;
        }
        let split_at = self.spans.len() / 2;
        let left_spans = &self.spans[..split_at];
        let right_spans = &self.spans[split_at..];
        Some((
            self.slice_from_spans(left_spans),
            self.slice_from_spans(right_spans),
        ))
    }

    fn slice_from_spans(&self, spans: &[BatchRecordSpan]) -> Batch {
        let body_start = spans.first().unwrap().body_start;
        let body_end = spans.last().unwrap().body_end;
        let body = self.body.slice(body_start..body_end);
        let normalized_spans = spans
            .iter()
            .map(|span| BatchRecordSpan {
                body_start: span.body_start - body_start,
                body_end: span.body_end - body_start,
                start_offset: span.start_offset,
                end_offset: span.end_offset,
            })
            .collect::<Vec<_>>();
        Batch {
            byte_count: body.len(),
            body,
            record_count: spans.len(),
            start_offset: spans.first().unwrap().start_offset,
            end_offset: spans.last().unwrap().end_offset,
            spans: normalized_spans,
        }
    }
}

#[derive(Debug)]
pub struct Batcher {
    config: BatcherConfig,
    body: BytesMut,
    record_count: usize,
    start_offset: Option<u64>,
    end_offset: u64,
    spans: Vec<BatchRecordSpan>,
}

impl Batcher {
    pub fn new(config: BatcherConfig) -> Self {
        let initial_capacity = config.max_batch_bytes.min(1024 * 1024);
        Self {
            config,
            body: BytesMut::with_capacity(initial_capacity),
            record_count: 0,
            start_offset: None,
            end_offset: 0,
            spans: Vec::new(),
        }
    }

    pub fn update_max_batch_bytes(&mut self, max_batch_bytes: usize) {
        self.config.max_batch_bytes = max_batch_bytes;
    }

    pub fn push(&mut self, record: Record) -> Option<Batch> {
        let record_len_with_newline = record.bytes.len() + 1;
        let should_flush_first = self.record_count > 0
            && (self.record_count >= self.config.max_batch_records
                || self.body.len() + record_len_with_newline > self.config.max_batch_bytes);

        let flushed = if should_flush_first {
            self.flush()
        } else {
            None
        };

        if self.start_offset.is_none() {
            self.start_offset = Some(record.start_offset);
        }
        self.end_offset = record.end_offset;
        self.record_count += 1;
        let body_start = self.body.len();
        self.body.put_slice(&record.bytes);
        self.body.put_u8(b'\n');
        let body_end = self.body.len();
        self.spans.push(BatchRecordSpan {
            body_start,
            body_end,
            start_offset: record.start_offset,
            end_offset: record.end_offset,
        });
        flushed
    }

    pub fn flush(&mut self) -> Option<Batch> {
        let start_offset = self.start_offset?;
        let body = self.body.split().freeze();
        let spans = std::mem::take(&mut self.spans);
        let batch = Batch {
            byte_count: body.len(),
            body,
            record_count: self.record_count,
            start_offset,
            end_offset: self.end_offset,
            spans,
        };

        self.record_count = 0;
        self.start_offset = None;
        self.end_offset = 0;
        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn record(text: &str, start: u64, end: u64) -> Record {
        Record {
            bytes: Bytes::copy_from_slice(text.as_bytes()),
            start_offset: start,
            end_offset: end,
        }
    }

    #[test]
    fn flushes_records_into_ndjson_body() {
        let mut batcher = Batcher::new(BatcherConfig {
            max_batch_bytes: 1024,
            max_batch_records: 10,
        });
        assert!(batcher.push(record("a", 0, 2)).is_none());
        assert!(batcher.push(record("b", 2, 4)).is_none());

        let batch = batcher.flush().unwrap();
        assert_eq!(&batch.body[..], b"a\nb\n");
        assert_eq!(batch.record_count, 2);
        assert_eq!(batch.start_offset, 0);
        assert_eq!(batch.end_offset, 4);
    }

    #[test]
    fn returns_full_batch_when_record_limit_is_reached() {
        let mut batcher = Batcher::new(BatcherConfig {
            max_batch_bytes: 1024,
            max_batch_records: 2,
        });
        assert!(batcher.push(record("a", 0, 2)).is_none());
        assert!(batcher.push(record("b", 2, 4)).is_none());
        let full = batcher.push(record("c", 4, 6)).unwrap();

        assert_eq!(&full.body[..], b"a\nb\n");
        let tail = batcher.flush().unwrap();
        assert_eq!(&tail.body[..], b"c\n");
    }

    #[test]
    fn returns_full_batch_when_byte_limit_would_be_exceeded() {
        let mut batcher = Batcher::new(BatcherConfig {
            max_batch_bytes: 4,
            max_batch_records: 10,
        });
        assert!(batcher.push(record("aa", 0, 3)).is_none());
        let full = batcher.push(record("bb", 3, 6)).unwrap();

        assert_eq!(&full.body[..], b"aa\n");
    }
    #[test]
    fn batch_splits_into_two_valid_ndjson_batches() {
        let mut batcher = Batcher::new(BatcherConfig {
            max_batch_bytes: 1024,
            max_batch_records: 10,
        });
        batcher.push(record("a", 0, 2));
        batcher.push(record("b", 2, 4));
        let batch = batcher.flush().unwrap();
        let (left, right) = batch.split_half().unwrap();

        assert_eq!(&left.body[..], b"a\n");
        assert_eq!(&right.body[..], b"b\n");
        assert_eq!(left.start_offset, 0);
        assert_eq!(right.end_offset, 4);
    }
}
