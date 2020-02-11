use memmap::MmapOptions;
use tdb_core::dtf::{self, file_format as ff};
use tdb_core::postprocessing::candle::time_bars::TimeBars;
use std::fs::File;
use indicatif::{ProgressBar, ProgressStyle};

pub fn run(matches: &clap::ArgMatches) {
    // single file
    let input = matches.value_of("input").unwrap_or("");
    // or range
    let symbol = matches.value_of("symbol").unwrap_or("");
    let min = matches.value_of("min").unwrap_or("0").parse().unwrap();
    let max = matches.value_of("max").unwrap_or("9999999999999").parse().unwrap();
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
    let has_output = matches.is_present("output");
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

                let mut ret = vec![];
                for (i, up) in &mut it
                    .enumerate()
                {
                    if up.ts > max { break; }
                    if up.ts < min { continue; }
                    if i != 0 && i % 10000 == 0 { bar.inc(10000); }
                    if has_output {
                        ret.push(up);
                    } else if csv {
                        println!("{}", up.to_csv()) // TODO: slooooow
                    } else {
                        println!("[{}]", up.as_json())
                    }
                }
                bar.finish();

                if has_output {
                    let fname = matches.value_of("output").unwrap();
                    ff::encode(fname, symbol, &ret).unwrap();
                }
            }
        }
    } else {
        if print_metadata {
            println!("total updates in folder: {}",
                tdb_core::storage::utils::print_total_folder_updates_len(folder).unwrap());
        } else {
            if timebars {
                let ups = tdb_core::dtf::file_format::scan_files_for_range(
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
                let mut ret = vec![];
                tdb_core::dtf::file_format::scan_files_for_range_for_each(
                    &folder,
                    &symbol,
                    min,
                    max,
                    &mut |up| {
                        if has_output {
                            ret.push(*up);
                        } else if csv {
                            println!("{}", up.to_csv())
                        } else {
                            println!("[{}]", up.as_json())
                        }
                    }).unwrap();

                if has_output {
                    let fname = matches.value_of("output").unwrap();
                    ff::encode(fname, symbol, &ret).unwrap();
                }

            }
        }
    };


}