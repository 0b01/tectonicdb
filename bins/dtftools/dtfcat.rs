use memmap::MmapOptions;
use tdb_core::dtf::{self, file_format as ff};
use tdb_core::postprocessing::candle::time_bars::TimeBars;
use std::fs::File;
use std::borrow::Cow;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::mpsc::channel;
use std::thread;

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

                let (send, recv) = channel();
                let thr = thread::spawn(move || {
                    let bar = ProgressBar::new(meta.count);
                    bar.set_style(ProgressStyle::default_bar()
                        .template("[{elapsed_precise}, remaining: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                        .progress_chars("##-"));
                    let mut len = 0;
                    let mut last_ts = 0;
                    for (i, up) in &mut it
                      .enumerate()
                    {
                        if up.ts > max { break; }
                        if up.ts < min { continue; }
                        len += 1;
                        last_ts = up.ts;
                        if i != 0 && i % 10000 == 0 { bar.inc(10000); }
                        if has_output {
                            send.send(up).unwrap();
                        } else if csv {
                            println!("{}", up.to_csv()) // TODO: slooooow
                        } else {
                            println!("[{}]", up.as_json())
                        }
                    }
                    bar.finish();
                    (len, last_ts)
                });

                if has_output {
                    let outfname = matches.value_of("output").unwrap();
                    let mut wtr = dtf::file_format::file_writer(outfname, true).unwrap();
                    ff::write_magic_value(&mut wtr).unwrap();
                    ff::write_symbol(&mut wtr, &symbol).unwrap();
                    let mut it = recv.iter();
                    let t = it.by_ref().map(Cow::Owned).peekable();
                    ff::write_main(&mut wtr, t).unwrap();

                    let (len, last_ts) = thr.join().unwrap();
                    ff::write_len(&mut wtr, len).unwrap();
                    ff::write_max_ts(&mut wtr, last_ts).unwrap();
                } else {
                    thr.join().unwrap();
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
                let folder_ = folder.to_owned();
                let symbol_ = symbol.to_owned();
                let (send, recv) = channel();

                let thr = thread::spawn(move || {
                    let mut len = 0;
                    let mut last_ts = 0;
                    tdb_core::dtf::file_format::scan_files_for_range_for_each(
                        &folder_,
                        &symbol_,
                        min,
                        max,
                        &mut |up| {
                            if has_output {
                                send.send(*up).unwrap();
                            } else if csv {
                                println!("{}", up.to_csv())
                            } else {
                                println!("[{}]", up.as_json())
                            }
                            len += 1;
                            last_ts = up.ts;
                        }).unwrap();
                    (len, last_ts)
                });

                if has_output {
                    let outfname = matches.value_of("output").unwrap();
                    let mut wtr = dtf::file_format::file_writer(outfname, true).unwrap();
                    ff::write_magic_value(&mut wtr).unwrap();
                    ff::write_symbol(&mut wtr, &symbol).unwrap();
                    let mut it = recv.iter();
                    let t = it.by_ref().map(Cow::Owned).peekable();
                    ff::write_main(&mut wtr, t).unwrap();

                    let (len, last_ts) = thr.join().unwrap();
                    ff::write_len(&mut wtr, len).unwrap();
                    ff::write_max_ts(&mut wtr, last_ts).unwrap();
                } else {
                    thr.join().unwrap();
                }

            }
        }
    };


}