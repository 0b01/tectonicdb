// mod write_npy;
use zip::write::ZipWriter;
use std::path::Path;

use memmap::MmapOptions;
use std::fs::File;
use libtectonic::dtf;
use indicatif::{ProgressBar, ProgressStyle};

pub fn run(matches: &clap::ArgMatches) {
    let input = matches.value_of("input").unwrap_or("");
    if input != "" {

        let file = File::open(input).unwrap();
        let rdr = unsafe { MmapOptions::new().map(&file).unwrap() };
        let rdr = std::io::Cursor::new(rdr);

        let out_fname = Path::new(input).with_extension(".npz");
        let mut zip = ZipWriter::new(File::open(out_fname).unwrap());

        let it = dtf::file_format::iterators::DTFBufReader::new(rdr);
        let bar = ProgressBar::new(it.current_update_index().into());
        bar.set_style(ProgressStyle::default_bar()
            .template("[{elapsed_precise}, remaining: {eta_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .progress_chars("##-"));

        for (i, up) in it.enumerate() {
            if i != 0 && i % 10000 == 0 {
                bar.inc(10000);
            }
            //

        }
        bar.finish();
    }
}
