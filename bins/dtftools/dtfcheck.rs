use memmap::MmapOptions;
use libtectonic::dtf;
use std::fs::File;

pub fn run(matches: &clap::ArgMatches) {
    let input = matches.value_of("input").unwrap();
    let threshold: i64 = matches.value_of("threshold").unwrap_or("60").parse().unwrap();

    let file = File::open(input).unwrap();
    let rdr = unsafe { MmapOptions::new().map(&file).unwrap() };
    let rdr = std::io::Cursor::new(rdr);

    let mut it = dtf::file_format::iterators::DTFBufReader::new(rdr);
    let mut prev: Option<dtf::update::Update> = None;
    for up in &mut it {
        if prev.is_some() {
            let prev = prev.unwrap();
            let gap = (up.ts as i64 - prev.ts as i64).abs();
            if gap > threshold * 1000 {
                let upts = libtectonic::utils::epoch_to_human(up.ts/1000);
                let prevts = libtectonic::utils::epoch_to_human(prev.ts/1000);
                println!("Gap detected: {} = {} - {}, {} - {}", gap, prev.ts, up.ts, prevts, upts);
            }
        }
        prev = Some(up);
    }
}