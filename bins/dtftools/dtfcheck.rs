use memmap::MmapOptions;
use tdb_core::dtf;
use std::fs::File;
use indicatif::{ProgressBar, ProgressStyle};

pub fn run(matches: &clap::ArgMatches) {
    let input = matches.value_of("input").unwrap();
    let threshold: i64 = matches.value_of("threshold").unwrap_or("60").parse().unwrap();

    let file = File::open(input).unwrap();
    let rdr = unsafe { MmapOptions::new().map(&file).unwrap() };
    let mut rdr = std::io::Cursor::new(rdr);

    let meta = dtf::file_format::read_meta_from_buf(&mut rdr).unwrap();
    let bar = ProgressBar::new(meta.count);
    bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}, remaining: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-"));

    let mut it = dtf::file_format::iterators::DTFBufReader::new(rdr);
    let mut prev: Option<dtf::update::Update> = None;
    for (i, up) in &mut it.enumerate() {
        if i != 0 && i % 10000 == 0 { bar.inc(10000); }
        if prev.is_some() {
            let prev = prev.unwrap();
            let gap = (up.ts as i64 - prev.ts as i64).abs();
            if gap > threshold * 1000 {
                let upts = tdb_core::utils::epoch_to_human(up.ts/1000);
                let prevts = tdb_core::utils::epoch_to_human(prev.ts/1000);
                println!("Gap detected: {} = {} - {}, {} - {}", gap, prev.ts, up.ts, prevts, upts);
            }
        }
        prev = Some(up);
    }
    bar.finish();
}