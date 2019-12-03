//! Given two DTF files, combines the data within them and outputs a single DTF file that contains
//! the data from both of them after discarding any duplicate updates.

use std::collections::HashSet;
use std::process::exit;
use libtectonic::dtf::{self, update::Update};
use libtectonic::dtf::file_format::Metadata;

const USAGE: &'static str = "Usage: `dtfconcat input1 input2 output`";
const DTF_ERROR: &'static str = "Unable to parse input DTF file!";

pub fn run(matches: &clap::ArgMatches) {
    let input1_filename = matches
        .value_of("input1")
        .expect(USAGE);
    let input2_filename = matches
        .value_of("input2")
        .expect(USAGE);
    let output_filename = matches
        .value_of("output")
        .expect(USAGE);
    // Get metadata for both of the input files to determine which one starts first
    let input1_metadata = dtf::file_format::read_meta(input1_filename).expect(DTF_ERROR);
    let input2_metadata = dtf::file_format::read_meta(input2_filename).expect(DTF_ERROR);
    // Sanity checks to make sure they're the same symbol and continuous
    if input1_metadata.symbol != input2_metadata.symbol {
        println!(
            "ERROR: The two input files provided have different symbols: {}, {}",
            input1_metadata.symbol,
            input2_metadata.symbol
        );
        exit(1);
    }
    let (start_filename, start_metadata, end_filename, end_metadata) = if input1_metadata.min_ts > input2_metadata.min_ts {
        (input1_filename, input1_metadata, input2_filename, input2_metadata)
    } else {
        (input2_filename, input2_metadata, input1_filename, input1_metadata)
    };
    match combine_files(start_filename, start_metadata, end_filename, end_metadata, output_filename) {
        Ok(()) => println!("Successfully merged files and output to {}", output_filename),
        Err(err) => {
            println!("{}", err);
            exit(1);
        }
    }
}

pub fn combine_files(
    start_filename: &str,
    start_metadata: Metadata,
    end_filename: &str,
    end_metadata: Metadata,
    output_filename: &str
) -> Result<(), String> {
    if start_metadata.max_ts < end_metadata.min_ts {
        return Err("ERROR: The provided input files are not continuous!".into());
    }

    // println!("START METADATA: {:?}\nEND METADATA: {:?}", start_metadata, end_metadata);

    let symbol = start_metadata.symbol.clone();

    // Read updates from the start file until the `start_ts` of the second file is reached
    // let file1_updates: Vec<Update> = dtf::get_range_in_file(
    //     start_filename,
    //     start_metadata.min_ts,
    //     start_metadata.max_ts - 1
    // ).map_err(|_| DTF_ERROR)?;
    let full_file1 = dtf::file_format::decode(start_filename, None).map_err(|_| DTF_ERROR)?;
    let file1_updates: Vec<Update> = full_file1
        .iter()
        .filter(|&&Update { ts, .. }| ts >= start_metadata.min_ts && ts < start_metadata.max_ts)
        .cloned()
        .collect();

    // println!("FILE1 UPDATES: {:?}", file1_updates);

    // Read updates from the millisecond of overlap between the two files
    // let mut overlap_updates_1: Vec<Update> = dtf::get_range_in_file(
    //     start_filename,
    //     start_metadata.max_ts,
    //     start_metadata.max_ts
    // ).map_err(|_| DTF_ERROR)?;
    let mut overlap_updates_1: Vec<Update> = full_file1
        .iter()
        .filter(|&&Update { ts, .. }| ts == start_metadata.max_ts)
        .cloned()
        .collect();
    drop(full_file1);
    let full_file2 = dtf::file_format::decode(end_filename, None).map_err(|_| DTF_ERROR)?;
    // let mut overlap_updates_2: Vec<Update> = dtf::get_range_in_file(
    //     end_filename,
    //     start_metadata.max_ts,
    //     start_metadata.max_ts
    // ).map_err(|_| DTF_ERROR)?;
    let mut overlap_updates_2: Vec<Update> = full_file2
        .iter()
        .filter(|&&Update { ts, .. }| ts == start_metadata.max_ts)
        .cloned()
        .collect();
    overlap_updates_1.append(&mut overlap_updates_2);

    // We have to deduplicate with a `HashSet` because `Update`s `Ord` implementation
    // doesn't look at anything except timestamp.
    // We have to serialize before storing because Rust doesn't let us hash floating
    // point numbers because of `NaN`.
    let mut overlapping_updates: HashSet<String> = overlap_updates_1
        .iter()
        .map(serde_json::to_string)
        .map(Result::unwrap)
        .collect();
    let mut overlapping_updates: Vec<Update> = overlapping_updates
        .drain()
        .map(|s| serde_json::from_str(&s).unwrap())
        .collect();
    overlapping_updates.sort();

    // println!("OVERLAP UPDATES: {:?}", overlapping_updates);

    // Read updates from the second file starting where the first file left off
    // let mut file2_updates: Vec<Update> = dtf::get_range_in_file(
    //     end_filename,
    //     start_metadata.max_ts + 1,
    //     end_metadata.max_ts
    // ).map_err(|_| DTF_ERROR)?;
    let mut file2_updates: Vec<Update> = full_file2
        .iter()
        .filter(|&&Update { ts, .. }| ts >= start_metadata.max_ts + 1)
        .cloned()
        .collect();
    drop(full_file2);

    // println!("FILE2 UPDATES: {:?}", file2_updates);

    // Concat the buffers together, deduplicate, and output into a DTF file
    let mut joined_updates = file1_updates;
    joined_updates.append(&mut overlapping_updates);
    joined_updates.append(&mut file2_updates);

    dtf::file_format::encode(output_filename, &symbol, &joined_updates)
        .map_err(|_| String::from("Error while writing output file!"))?;

    Ok(())
}

#[test]
fn dtf_merging() {
    use std::fs::remove_file;

    let mut update_timestamps_1: Vec<u64> = (0..1000).collect();
    update_timestamps_1.append(
        &mut vec![1001, 1002, 1003, 1004, 1004, 1007, 1008, 1009, 1009, 1010]
    );
    let update_timestamps_2: &[u64] = &[1008, 1009, 1009, 1010, 1010, 1011, 1012];

    let map_into_updates = |timestamps: &[u64], seq_offset: usize| -> Vec<Update> {
        let mut last_timestamp = 0;

        timestamps
            .into_iter()
            .enumerate()
            .map(|(i, ts)| {
                let update = Update {
                    ts: *ts,
                    seq: i as u32 + seq_offset as u32,
                    is_trade: false,
                    is_bid: true,
                    price: *ts as f32 + if last_timestamp == *ts { 1. } else { 0. },
                    size: *ts as f32,
                };

                last_timestamp = *ts;

                update
            })
            .collect()
    };

    // Generate test data
    let updates1 = map_into_updates(&update_timestamps_1, 0);
    let updates2 = map_into_updates(update_timestamps_2, 1006);

    // Write into DTF files
    let filename1 = "./test/test-data/dtfconcat1.dtf";
    let filename2 = "./test/test-data/dtfconcat2.dtf";
    let output_filename = "./test/test-data/dtfconcat_out.dtf";

    dtf::file_format::encode(filename1, "test", &updates1).unwrap();
    dtf::file_format::encode(filename2, "test", &updates2).unwrap();

    let metadata1 = dtf::file_format::read_meta(filename1).unwrap();
    let metadata2 = dtf::file_format::read_meta(filename2).unwrap();

    let expected_ts_price: &[(u64, f32)] = &[
        (1001, 1001.),
        (1002, 1002.),
        (1003, 1003.),
        (1004, 1004.),
        (1004, 1005.),
        (1007, 1007.),
        (1008, 1008.),
        (1009, 1009.),
        (1009, 1010.),
        (1010, 1010.),
        (1010, 1011.),
        (1011, 1011.),
        (1012, 1012.),
    ];

    // Concat the files and verify that they contain the correct data
    combine_files(filename1, metadata1, filename2, metadata2, output_filename).unwrap();
    let merged_updates: Vec<Update> = dtf::file_format::decode(output_filename, None).unwrap();

    remove_file(filename1).unwrap();
    remove_file(filename2).unwrap();
    remove_file(output_filename).unwrap();

    let actual_ts_price: Vec<(u64, f32)> = merged_updates
        .into_iter()
        .skip(1000)
        .map(|Update { ts, price, .. }| (ts, price))
        .collect();

    assert_eq!(expected_ts_price, actual_ts_price.as_slice());
}
