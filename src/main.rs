use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::ops::Range;
use std::sync::Arc;

use memmap2::MmapOptions;

const FILE_PATH: &str = "measurements.txt";

#[derive(Debug)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f64,
    count: usize,
}

fn main() {
    let num_threads = std::thread::available_parallelism().unwrap();

    let file = File::open(FILE_PATH).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let mmap = Arc::new(mmap);
    let file_size = mmap.len();
    let chunk_size = file_size / num_threads;

    let mut i = chunk_size;
    let mut indices = vec![0];
    while i < file_size {
        while i < file_size && mmap[i] != b'\n' {
            i += 1;
        }
        if i < file_size {
            i += 1;
        }
        indices.push(i);
        i += chunk_size;
    }

    let buckets: Vec<Range<usize>> = indices.windows(2).map(|w| w[0]..w[1]).collect();

    let mut handles = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_iter() {
        let mmap = Arc::clone(&mmap);
        let h = std::thread::spawn(move || {
            let chunk = &mmap[bucket.start..bucket.end];
            let mut measurements = HashMap::new();
            let mut start = 0;
            let mut i = 0;

            // TODO: check rqbit stats reporting.
            // How to process the op/s for a workload that is distributed
            // across several threads.
            while i < chunk.len() {
                if chunk[i] != b'\n' {
                    i += 1;
                    continue;
                }

                process_record(&chunk[start..i], &mut measurements).unwrap();

                i += 1;
                start = i;
            }

            measurements
        });
        handles.push(h);
    }

    // Use the map from the first thread to gather the results from all threads.
    let mut results = handles.pop().unwrap().join().unwrap();
    for h in handles {
        let measure = h.join().unwrap();
        for (c, m) in measure.into_iter() {
            match results.get_mut(&c) {
                Some(total) => {
                    total.sum += m.sum;
                    total.count += m.count;
                    total.min = f32::min(total.min, m.min);
                    total.max = f32::max(total.max, m.max);
                }
                None => {
                    results.insert(c, m);
                }
            }
        }
    }

    let mut results: Vec<(Vec<u8>, Measurement)> = results.into_iter().collect();
    results.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(b"{").unwrap();

    for (cname, measure) in results {
        let avg = measure.sum / measure.count as f64;
        handle.write_all(&cname).unwrap();
        write!(
            handle,
            "={:.1}/{:.1}/{:.1}, ",
            measure.min, avg, measure.max
        )
        .unwrap();
    }
    handle.write_all(b"}").unwrap();
}

// TODO: mmap is wrapped in Arc. We probably can change the keys of
// the map to &[u8] to reduce allocations.
fn process_record(
    record: &[u8],
    measurements: &mut HashMap<Vec<u8>, Measurement>,
) -> Result<(), String> {
    let mut i = 0;
    while record[i] != b';' {
        i += 1;
    }

    // Excluding the ';'
    let city = &record[..i];

    // After ';' to (excluding) '\n'
    let sample: f32 = parse_f32(&record[i + 1..record.len()]);

    if let Some(m) = measurements.get_mut(city) {
        if sample < m.min {
            m.min = sample;
        }
        if m.max < sample {
            m.max = sample;
        }
        m.sum += sample as f64;
        m.count += 1;
        return Ok(());
    }

    let m = Measurement {
        min: sample,
        max: sample,
        sum: sample as f64,
        count: 1,
    };
    measurements.insert(city.to_vec(), m);

    Ok(())
}

fn parse_f32(b: &[u8]) -> f32 {
    let mut i = 0;
    let mut int = 0i32;

    let mut is_neg = false;
    if b[i] == b'-' {
        is_neg = true;
        i += 1;
    }
    while b[i] != b'.' {
        let digit = b[i] - b'0';
        int = int * 10 + digit as i32;
        i += 1;
    }

    // There's only one digit after '.'
    let digit = b[i + 1] - b'0';
    let frac: f32 = digit as f32 / 10.0;
    let mut result = int as f32 + frac;
    if is_neg {
        result = -result;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_f32() {
        // "b-0.1".as_ref()
        // &b"-50.2"[..]
        assert_eq!(parse_f32(b"-0.1"), -0.1);
        assert_eq!(parse_f32(b"-50.2"), -50.2);
        assert_eq!(parse_f32(b"-66.9"), -66.9);
        assert_eq!(parse_f32(b"43.4"), 43.4);
        assert_eq!(parse_f32(b"92.2"), 92.2);
    }
}
