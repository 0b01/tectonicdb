use std::io::{Write, BufWriter};
use zip::write::{ZipWriter, FileOptions};
use zip::CompressionMethod;

use std::path::Path;
use std::fs::File;

use memmap::MmapOptions;

use tdb_core::dtf;
use indicatif::{ProgressBar, ProgressStyle};

pub fn run(matches: &clap::ArgMatches) -> Option<()> {
    let input = matches.value_of("input").unwrap_or("");
    let compression = if matches.is_present("compressed") {
        CompressionMethod::Deflated
    } else {
        CompressionMethod::Stored
    };
    if input != "" {
        let file = File::open(input).unwrap();
        let rdr = unsafe { MmapOptions::new().map(&file).unwrap() };
        let mut rdr = std::io::Cursor::new(rdr);

        // output file is the same name except with npz extension
        let out_fname = Path::new(input).with_extension("npz");
        let mut zip = BufWriter::new(ZipWriter::new(File::create(out_fname).unwrap()));

        let meta = dtf::file_format::read_meta_from_buf(&mut rdr).ok()?;

        let mut it = dtf::file_format::iterators::DTFBufReader::new(rdr);
        let bar = ProgressBar::new(meta.count * 6);
        bar.set_style(ProgressStyle::default_bar()
            .template("[{elapsed_precise}, remaining: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"));

        macro_rules! write_arr {
            (bool $name:expr, $fmt:expr, $e:ident) => {
                zip.get_mut().start_file($name, FileOptions::default().compression_method(compression)).ok()?;
                write_header(&mut zip, $fmt, meta.count);
                for (i, up) in &mut it.enumerate() {
                    if i != 0 && i % 10000 == 0 { bar.inc(10000); }
                    if up.$e {
                        zip.write(&1u8.to_le_bytes()).ok()?;
                    } else {
                        zip.write(&0u8.to_le_bytes()).ok()?;
                    }
                }
                it.reset();
                zip.flush().ok()?;
            };

            (num $name:expr, $fmt:expr, $e:ident) => {
                zip.get_mut().start_file($name, FileOptions::default().compression_method(compression)).ok()?;
                write_header(&mut zip, $fmt, meta.count);
                for (i, up) in &mut it.enumerate() {
                    if i != 0 && i % 10000 == 0 { bar.inc(10000); }
                    zip.write(&up.$e.to_le_bytes()).ok()?;
                }
                it.reset();
                zip.flush().ok()?;
            }
        };

        write_arr!(num "ts", "<i8",    ts);
        write_arr!(num "seq", "<i4",   seq);
        write_arr!(num "price", "<f4", price);
        write_arr!(num "size", "<f4",  size);
        write_arr!(bool "is_bid", "?",  is_bid);
        write_arr!(bool "is_trade", "?",is_trade);


        bar.finish();
    }

    Some(())
}

static MAGIC_VALUE: &[u8] = &[0x93, 0x4E, 0x55, 0x4D, 0x50, 0x59];

pub fn write_header(wtr: &mut dyn Write, numpy_type: &str, len: u64) {
    let _ = wtr.write(MAGIC_VALUE);
    let _ = wtr.write(&[0x01, 0x00]); // major version, minor version
    let header = format!(
        "{{'descr':[('data','{}')],'fortran_order':False,'shape':({},)}}",
        numpy_type, len
    );
    let header_len = header.len();
    let _ = wtr.write(&(header_len as u16).to_le_bytes());
    let _ = wtr.write(header.as_bytes()); // header
}