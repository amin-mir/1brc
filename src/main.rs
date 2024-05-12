use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::num::ParseFloatError;
use std::time::Instant;

fn main() {
    let file = File::open("measurements.txt").unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut measurements = HashMap::new();

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

        process_record(&line, &mut measurements).unwrap();
        line.clear();

        processed += 1;
        if processed % 1_000_000 == 0 {
            let ops_per_ms = (processed - last_processed) / ts.elapsed().as_millis();
            println!("op/s: {}", ops_per_ms * 1000);

            ts = Instant::now();
            last_processed = processed;
        }
    }

    let mut results: Vec<(String, Measurement)> = Vec::new();
    results.extend(measurements.into_iter());

    results.sort_by(|a, b| a.0.cmp(&b.0));
    for m in results.iter().take(30) {
        println!("city: {}, measurement: {:?}", m.0, m.1);
    }
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
        m.sum += sample;
        m.count += 1;
        return Ok(());
    }

    let m = Measurement {
        min: sample,
        max: sample,
        sum: sample,
        count: 1,
    };
    measurements.insert(city.to_string(), m);

    Ok(())
}

#[derive(Debug)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f32,
    count: usize,
}
