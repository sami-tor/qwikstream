use bytes::Bytes;
use memchr::memchr_iter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub bytes: Bytes,
    pub start_offset: u64,
    pub end_offset: u64,
}

#[derive(Debug, Default)]
pub struct LineSplitter {
    carry: Vec<u8>,
    carry_start_offset: Option<u64>,
}

impl LineSplitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_chunk(&mut self, chunk: &[u8], chunk_start_offset: u64) -> Vec<Record> {
        let mut records = Vec::new();
        self.push_chunk_for_each(chunk, chunk_start_offset, |record| records.push(record));
        records
    }

    pub fn push_chunk_for_each<F>(&mut self, chunk: &[u8], chunk_start_offset: u64, mut emit: F)
    where
        F: FnMut(Record),
    {
        let mut segment_start = 0_usize;
        let mut absolute_segment_start = chunk_start_offset;

        if !self.carry.is_empty() {
            absolute_segment_start = self.carry_start_offset.unwrap_or(chunk_start_offset);
        }

        for idx in memchr_iter(b'\n', chunk) {
            let line_end = idx;
            let record_start = absolute_segment_start;
            let record_end = chunk_start_offset + idx as u64 + 1;

            if self.carry.is_empty() {
                let line = trim_carriage_return(&chunk[segment_start..line_end]);
                if !line.is_empty() {
                    emit(Record {
                        bytes: Bytes::copy_from_slice(line),
                        start_offset: record_start,
                        end_offset: record_end,
                    });
                }
            } else {
                self.carry
                    .extend_from_slice(&chunk[segment_start..line_end]);
                let line = trim_carriage_return(&self.carry).to_vec();
                if !line.is_empty() {
                    emit(Record {
                        bytes: Bytes::from(line),
                        start_offset: record_start,
                        end_offset: record_end,
                    });
                }
                self.carry.clear();
                self.carry_start_offset = None;
            }

            segment_start = idx + 1;
            absolute_segment_start = chunk_start_offset + segment_start as u64;
        }

        if segment_start < chunk.len() {
            if self.carry.is_empty() {
                self.carry_start_offset = Some(chunk_start_offset + segment_start as u64);
            }
            self.carry.extend_from_slice(&chunk[segment_start..]);
        }
    }

    pub fn finish(&mut self, final_offset: u64) -> Option<Record> {
        if self.carry.is_empty() {
            return None;
        }
        let start_offset = self.carry_start_offset.unwrap_or(final_offset);
        let line = trim_carriage_return(&self.carry).to_vec();
        self.carry.clear();
        self.carry_start_offset = None;
        if line.is_empty() {
            None
        } else {
            Some(Record {
                bytes: Bytes::from(line),
                start_offset,
                end_offset: final_offset,
            })
        }
    }
}

fn trim_carriage_return(bytes: &[u8]) -> &[u8] {
    bytes.strip_suffix(b"\r").unwrap_or(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_lines_with_offsets() {
        let mut splitter = LineSplitter::new();
        let records = splitter.push_chunk(b"one\ntwo\n", 0);

        assert_eq!(records.len(), 2);
        assert_eq!(&records[0].bytes[..], b"one");
        assert_eq!(records[0].start_offset, 0);
        assert_eq!(records[0].end_offset, 4);
        assert_eq!(&records[1].bytes[..], b"two");
        assert_eq!(records[1].start_offset, 4);
        assert_eq!(records[1].end_offset, 8);
    }

    #[test]
    fn handles_line_across_chunk_boundary() {
        let mut splitter = LineSplitter::new();
        assert!(splitter.push_chunk(b"hel", 0).is_empty());
        let records = splitter.push_chunk(b"lo\nnext", 3);
        let final_record = splitter.finish(10).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].bytes[..], b"hello");
        assert_eq!(records[0].start_offset, 0);
        assert_eq!(records[0].end_offset, 6);
        assert_eq!(&final_record.bytes[..], b"next");
        assert_eq!(final_record.start_offset, 6);
        assert_eq!(final_record.end_offset, 10);
    }

    #[test]
    fn streams_records_to_callback_without_collecting_in_splitter() {
        let mut splitter = LineSplitter::new();
        let mut records = Vec::new();
        splitter.push_chunk_for_each(
            b"one
two
",
            0,
            |record| records.push(record),
        );

        assert_eq!(records.len(), 2);
        assert_eq!(&records[0].bytes[..], b"one");
        assert_eq!(&records[1].bytes[..], b"two");
    }

    #[test]
    fn trims_windows_line_endings() {
        let mut splitter = LineSplitter::new();
        let records = splitter.push_chunk(b"one\r\n", 0);
        assert_eq!(&records[0].bytes[..], b"one");
    }
}
