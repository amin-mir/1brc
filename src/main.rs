use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasher, RandomState};
use std::io::{BufRead, BufReader};
use std::num::ParseFloatError;
use std::thread::JoinHandle;
use std::time::Instant;

use kanal;

const NUM_THREADS: u64 = 10;

fn main() {
    let file = File::open("measurements.txt").unwrap();
    let mut buf_reader = BufReader::new(file);

    let s = RandomState::new();
    let _ = s.hash_one("some text");

    let mut tasks: Vec<Task> = Vec::with_capacity(NUM_THREADS as usize);

    for _ in 0..NUM_THREADS {
        let (tx, rx) = kanal::unbounded();
        let h = std::thread::spawn(move || match process_measurements(rx) {
            Err(e) => {
                println!("thread errored: {}", e);
                Err(e)
            }
            Ok(r) => Ok(r),
        });
        tasks.push(Task {
            handle: h,
            tx: tx.clone(),
        })
    }

    // TODO: check rqbit stats reporting.
    let mut processed = 0;
    let mut last_processed = 0;
    let mut ts = Instant::now();
    loop {
        let mut line = String::new();
        let n = buf_reader.read_line(&mut line).unwrap();
        if n == 0 {
            break;
        }

        // Distribute the work between threads.
        let hash = s.hash_one(&line);
        let idx = hash % NUM_THREADS;

        tasks[idx as usize].tx.send(line).unwrap();

        processed += 1;
        if processed % 1_000_000 == 0 {
            let ops_per_ms = (processed - last_processed) / ts.elapsed().as_millis();
            println!("op/s: {}", ops_per_ms * 1000);

            ts = Instant::now();
            last_processed = processed;
        }
    }

    let mut results: Vec<(String, Measurement)> = Vec::new();
    for t in tasks {
        drop(t.tx);
        let res = t.handle.join().unwrap().unwrap();
        results.extend(res.into_iter());
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));
    // for m in results {
    //     println!("city: {}, measurement: {:?}", m.0, m.1);
    // }
}

fn process_measurements(
    rx: kanal::Receiver<String>,
) -> Result<HashMap<String, Measurement>, String> {
    let mut res = HashMap::new();

    while let Ok(measurement) = rx.recv() {
        let mut parts = measurement.split(';');
        let city = parts
            .next()
            .ok_or("missing city before comma in entry")
            .map_err(|e| e.to_string())?;
        let sample: f32 = parts
            .next()
            .map(|m| m.trim())
            .ok_or("missing measurement")?
            .parse()
            .map_err(|e: ParseFloatError| e.to_string())?;

        res.entry(city.to_string())
            .and_modify(|m: &mut Measurement| {
                if sample < m.min {
                    m.min = sample;
                }
                if m.max < sample {
                    m.max = sample;
                }
                m.sum += sample;
                m.count += 1;
            })
            .or_insert(Measurement {
                min: sample,
                max: sample,
                sum: sample,
                count: 1,
            });
    }

    Ok(res)
}

struct Task {
    handle: JoinHandle<Result<HashMap<String, Measurement>, String>>,
    tx: kanal::Sender<String>,
}

#[derive(Debug)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f32,
    count: usize,
}
