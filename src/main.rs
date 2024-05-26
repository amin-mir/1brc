use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::num::ParseFloatError;
use std::ops::Range;
use std::time::Instant;

const NUM_THREADS: u64 = 10;
const FILE_PATH: &str = "measurements.txt";

fn main() {
    let file = File::open(FILE_PATH).unwrap();
    let file_size = file.metadata().unwrap().len();
    let chunk_size = file_size / NUM_THREADS;

    let mut buf_reader = BufReader::new(file);
    let mut i = 0;
    let mut indices = vec![0];
    let mut buffer = Vec::with_capacity(1);
    loop {
        // Check if this is the last bucket.
        if i + chunk_size >= file_size {
            indices.push(file_size);
            break;
        }

        i = buf_reader
            .seek(SeekFrom::Current(chunk_size as i64 - 1))
            .unwrap();

        let n = buf_reader.read_until(b'\n', &mut buffer).unwrap();
        assert!(n != 0);

        // Since we're seeking by (chunk_size - 1), we could end up right on
        // a '\n' or in the middle of a record. So we try to advance i and
        // place it right at the beginning of the record.
        i += n as u64;
        indices.push(i);
    }

    let buckets: Vec<Range<u64>> = indices.windows(2).map(|w| (w[0]..w[1])).collect();
    println!("{:?}", buckets);

    let mut handles = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_iter() {
        let h = std::thread::spawn(move || {
            let file = File::open(FILE_PATH).unwrap();
            let mut buf_reader = BufReader::new(file);

            let mut pos = bucket.start;
            buf_reader.seek(SeekFrom::Start(pos)).unwrap();

            let mut measurements = HashMap::new();
            let mut line = String::new();

            // TODO: check rqbit stats reporting.
            // How to process the op/s for a workload that is distributed
            // across several threads.
            loop {
                let n = buf_reader.read_line(&mut line).unwrap();
                pos += n as u64;
                if pos == bucket.end {
                    println!("pos = {}, bucket end = {}", pos, bucket.end);
                    assert!(pos == bucket.end);
                    break;
                }

                process_record(&line, &mut measurements).unwrap();
                line.clear();
            }

            measurements
        });
        handles.push(h);
    }

    // Use the map from the first thread to gather the results from all threads.
    let mut results = handles.pop().unwrap().join().unwrap();
    let now = Instant::now();
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
    println!("final map has a len of {}", results.len());

    let mut results: Vec<(String, Measurement)> = results.into_iter().collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));
    let dur = now.elapsed().as_millis();
    println!("joining and sorting took {}ms", dur);

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(b"{").unwrap();

    for (cname, measure) in results {
        let avg = measure.sum / measure.count as f64;
        write!(
            handle,
            "{}={:.1}/{:.1}/{:.1}, ",
            cname, measure.min, avg, measure.max
        )
        .unwrap();
    }
    handle.write_all(b"}").unwrap();
}

fn process_record(
    record: &str,
    measurements: &mut HashMap<String, Measurement>,
) -> Result<(), String> {
    let mut parts = record.split(';');

    let city = parts
        .next()
        .ok_or("missing city before ';'")
        .map_err(|e| e.to_string())?;

    let sample: f32 = parts
        .next()
        .map(|m| m.trim_end())
        .ok_or("missing measurement after ';'")?
        .parse()
        .map_err(|e: ParseFloatError| e.to_string())?;

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
    measurements.insert(city.to_string(), m);

    Ok(())
}

#[derive(Debug)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f64,
    count: usize,
}
