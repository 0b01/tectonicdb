use memmap::MmapOptions;
use libtectonic::dtf;
use libtectonic::postprocessing::candle::time_bars::TimeBars;
use libtectonic::storage::utils::total_folder_updates_len;
use std::fs::File;
use indicatif::{ProgressBar, ProgressStyle};

pub fn run(matches: &clap::ArgMatches) {
    // single file
    let input = matches.value_of("input").unwrap_or("");
    // or range
    let symbol = matches.value_of("symbol").unwrap_or("");
    let min = matches.value_of("min").unwrap().parse().unwrap();
    let max = matches.value_of("max").unwrap().parse().unwrap();
    if min > max {
        println!("min must be smaller than max");
        ::std::process::exit(1);
    }
    let folder = matches.value_of("folder").unwrap_or("./");
    // candle
    let timebars = matches.is_present("timebars");
    let aligned = matches.is_present("aligned");
    let granularity = matches.value_of("minutes").unwrap_or("1");
    // misc
    let print_metadata = matches.is_present("meta");
    let csv = matches.is_present("csv");
    if input == "" && symbol == "" && folder == "" && !print_metadata {
        println!("Either supply a single file with -i or specify range.");
        ::std::process::exit(1);
    }
    if input != "" {
        if print_metadata {
            println!("{}", dtf::file_format::read_meta(input).unwrap());

            // let rdr = dtf::file_format::file_reader(input).expect("cannot open file");
            // for meta in dtf::file_format::iterators::DTFMetadataReader::new(rdr) {
            //     println!("{:?}", meta);
            // }
        } else {
            if timebars {
                let ups = dtf::file_format::decode(input, None).unwrap();
                let mut candles = TimeBars::from(ups.as_slice());
                candles.insert_continuation_candles();
                let rebinned = candles
                    .rebin(aligned, granularity.parse().unwrap())
                    .unwrap()
                    .to_csv();
                println!("{}", rebinned)
            } else {

                let file = File::open(input).unwrap();
                let rdr = unsafe { MmapOptions::new().map(&file).unwrap() };
                let mut rdr = std::io::Cursor::new(rdr);
                let meta = dtf::file_format::read_meta_from_buf(&mut rdr).unwrap();
                let mut it = dtf::file_format::iterators::DTFBufReader::new(rdr);
                let bar = ProgressBar::new(meta.count);
                bar.set_style(ProgressStyle::default_bar()
                    .template("[{elapsed_precise}, remaining: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                    .progress_chars("##-"));

                for (i, up) in &mut it.enumerate() {
                    if i != 0 && i % 10000 == 0 { bar.inc(10000); }
                    if csv {
                        println!("{}", up.to_csv()) // TODO: slooooow
                    } else {
                        println!("[{}]", up.as_json())
                    }
                }
                bar.finish();
            }
        }
    } else {
        if print_metadata {
            println!("total updates in folder: {}", total_folder_updates_len(folder).unwrap())
        } else {
            if timebars {
                let ups = libtectonic::dtf::file_format::scan_files_for_range(
                    folder,
                    symbol,
                    min,
                    max,
                    ).unwrap();
                let mut candles = TimeBars::from(ups.as_slice());
                candles.insert_continuation_candles();
                let rebinned = candles
                    .rebin(aligned, granularity.parse().unwrap())
                    .unwrap()
                    .to_csv();
                println!("{}", rebinned)
            } else {
                libtectonic::dtf::file_format::scan_files_for_range_for_each(
                    folder,
                    symbol,
                    min,
                    max,
                    &mut |up|{
                        if csv {
                            println!("{}", up.to_csv())
                        } else {
                            println!("[{}]", up.as_json())
                        }
                    }).unwrap();
            }
        }
    };
}