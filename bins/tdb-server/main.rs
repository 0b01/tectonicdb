#[macro_use]
extern crate log;
extern crate fern;

use tdb_server_core::prelude::*;
use clap::{Arg, App, ArgMatches};

fn main() {
    // Help detect OpenSSL certificates on Alpine Linux
    openssl_probe::init_ssl_cert_env_vars();
    let matches = get_matches();

    let host = matches
        .value_of("host")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_HOST", "0.0.0.0"));
    let port = matches
        .value_of("port")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_PORT", "9001"));
    let dtf_folder = matches
        .value_of("dtf_folder")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_DTF_FOLDER", "db"));
    let verbosity = matches.occurrences_of("v") as u8;
    let autoflush = {
        let cli_setting: bool = matches.is_present("autoflush");
        let env_setting = key_or_none("TDB_AUTOFLUSH");
        match env_setting {
            Some(s) => match s.as_ref() {
                "true" | "1" => true,
                "false" => false,
                _ => cli_setting,
            },
            None => cli_setting,
        }
    };
    let flush_interval = matches
        .value_of("flush_interval")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_FLUSH_INTERVAL", "1000"));
    let granularity = matches
        .value_of("granularity")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_GRANULARITY", "0"));
    let q_capacity = matches
        .value_of("q_capacity")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_Q_CAPACITY", "300"));

    let log_file = matches
        .value_of("log_file")
        .map(String::from)
        .unwrap_or_else(|| key_or_default("TDB_LOG_FILE_NAME", "tdb.log"));

    let influx = {
        #[cfg(feature = "influx")]
        {
            let influx_host = matches.value_of("influx_host") .map(String::from);
            let influx_db = matches.value_of("influx_db") .map(String::from);
            let influx_log_interval = matches.value_of("influx_log_interval").unwrap_or("60").parse().unwrap();
            match (influx_host, influx_db) {
                (Some(host), Some(db)) =>
                    Some(tdb_server_core::settings::InfluxSettings {
                        host,
                        db,
                        interval: influx_log_interval,
                    }),
                _ => None,
        }
        }
        #[cfg(not(feature = "influx"))]
        { None }
    };
    let settings = Arc::new(
        tdb_server_core::settings::Settings {
            autoflush,
            dtf_folder,
            flush_interval: flush_interval.parse().unwrap(),
            granularity: granularity.parse().unwrap(),
            q_capacity: q_capacity.parse().unwrap(),
            influx,
        }
    );


    prepare_logger(verbosity, &log_file);
    info!(r##"
           _/                            _/                          _/
        _/_/_/_/    _/_/      _/_/_/  _/_/_/_/    _/_/    _/_/_/          _/_/_/
         _/      _/_/_/_/  _/          _/      _/    _/  _/    _/  _/  _/
        _/      _/        _/          _/      _/    _/  _/    _/  _/  _/
         _/_/    _/_/_/    _/_/_/      _/_/    _/_/    _/    _/  _/    _/_/_/
    "##);

    task::block_on(tdb_server_core::server::run_server(&host, &port, settings)).unwrap();
}

fn prepare_logger(verbosity: u8, log_file: &str) {
    let level = match verbosity {
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Warn,
        2 => log::LevelFilter::Info,
        3 => log::LevelFilter::Debug,
        _ => log::LevelFilter::max(),
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S:%f]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(level)
        .level_for("hyper", log::LevelFilter::Info)
        .level_for("async_std", log::LevelFilter::Off)
        .chain(std::io::stdout())
        .chain(fern::log_file(log_file).unwrap())
        .apply()
        .unwrap();
}

/// Gets configuration values from CLI arguments, falling back to environment variables
/// if they don't exist and to default values if neither exist.
fn get_matches<'a>() -> ArgMatches<'a> {

    let app = App::new("tectonic-server")
        .version("1.0.0")
        .author("Ricky Han <tectonic@rickyhan.com>")
        .about("tectonic financial datastore")
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("HOST")
                .help("Sets the host to connect to (default 0.0.0.0)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Sets the port to connect to (default 9001)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dtf_folder")
                .short("f")
                .long("dtf_folder")
                .value_name("FOLDER")
                .help("Sets the folder to serve dtf files")
                .takes_value(true),
        )
        .arg(Arg::with_name("v").short("v").multiple(true).help(
            "Sets the level of verbosity",
        ))
        .arg(Arg::with_name("autoflush").short("a").help(
            "Sets autoflush (default is false)",
        ))

        .arg(
            Arg::with_name("flush_interval")
                .short("i")
                .long("flush_interval")
                .value_name("INTERVAL")
                .help("Sets autoflush interval (default every 1000 inserts)"),
        )
        .arg(
            Arg::with_name("granularity")
                .short("g")
                .long("granularity")
                .value_name("GRANULARITY")
                .help(
                    "Sets the history record granularity interval. (default 60s)",
                ),
        )
        .arg(
            Arg::with_name("log_file")
                .short("l")
                .long("log_file")
                .value_name("LOG_FILE")
                .help("Sets the log file to write to"),
        );

        let app = {
            #[cfg(feature="influx")]
            {
                app
                .arg(
                    Arg::with_name("influx_log_interval")
                        .takes_value(true)
                        .long("influx-log-interval")
                        .help( "influxdb log interval in seconds (default is 60)"))
                .arg(
                    Arg::with_name("influx_host")
                        .takes_value(true)
                        .long("influx-host")
                        .help( "influxdb host",)
                        .requires("influx_log_interval")
                        .requires("influx_db"))
                .arg(
                    Arg::with_name("influx_db")
                        .takes_value(true)
                        .long("influx-db")
                        .help( "influxdb db",)
                        .requires("influx_host"))
            }
            #[cfg(not(feature="influx"))]
            {
                app
            }
        };

        app.get_matches()
}