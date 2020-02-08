use std::io;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::error::Error;

use tdb_cli::client::TectonicClient;

use linefeed::{Interface, Prompter, ReadResult};
use linefeed::chars::escape_sequence;
use linefeed::command::COMMANDS;
use linefeed::complete::{Completer, Completion};
use linefeed::inputrc::parse_text;
use linefeed::terminal::Terminal;

const HISTORY_FILE: &str = "tdb.hst";

pub fn run(cli: &mut TectonicClient) -> io::Result<()> {
    let interface = Arc::new(Interface::new("demo")?);
    let mut thread_id = 0;

    println!("tdb cli");
    println!("Enter \"help\" for a list of commands.");
    println!("Press Ctrl-D or enter \"quit\" to exit.");
    println!("");

    interface.set_completer(Arc::new(DemoCompleter));
    interface.set_prompt("tdb> ")?;

    if let Err(e) = interface.load_history(HISTORY_FILE) {
        if e.kind() == io::ErrorKind::NotFound {
            println!("History file {} doesn't exist, not loading history.", HISTORY_FILE);
        } else {
            eprintln!("Could not load history file {}: {}", HISTORY_FILE, e);
        }
    }

    while let ReadResult::Input(line) = interface.read_line()? {
        if !line.trim().is_empty() {
            interface.add_history_unique(line.clone());
        }

        let (cmd, args) = split_first_word(&line);

        match cmd {
            "help" => {
                println!("linefeed demo commands:");
                println!();
                for &(cmd, help) in DEMO_COMMANDS {
                    println!("  {:15} - {}", cmd, help);
                }
                println!();
            }
            "bind" => {
                let d = parse_text("<input>", args);
                interface.evaluate_directives(d);
            }
            "get" => {
                if let Some(var) = interface.get_variable(args) {
                    println!("{} = {}", args, var);
                } else {
                    println!("no variable named `{}`", args);
                }
            }
            "list-bindings" => {
                for (seq, cmd) in interface.lock_reader().bindings() {
                    let seq = format!("\"{}\"", escape_sequence(seq));
                    println!("{:20}: {}", seq, cmd);
                }
            }
            "list-commands" => {
                for cmd in COMMANDS {
                    println!("{}", cmd);
                }
            }
            "list-variables" => {
                for (name, var) in interface.lock_reader().variables() {
                    println!("{:30} = {}", name, var);
                }
            }
            "spawn-log-thread" => {
                let my_thread_id = thread_id;
                println!("Spawning log thread #{}", my_thread_id);

                let iface = interface.clone();

                thread::spawn(move || {
                    let mut i = 0usize;
                    loop {
                        writeln!(iface, "[#{}] Concurrent message #{}",
                            my_thread_id, i).unwrap();
                        let wait_ms = 300;
                        thread::sleep(Duration::from_millis(wait_ms));
                        i += 1;
                    }
                });
                thread_id += 1;
            }
            "history" => {
                let w = interface.lock_writer_erase()?;

                for (i, entry) in w.history().enumerate() {
                    println!("{}: {}", i, entry);
                }
            }
            "save-history" => {
                if let Err(e) = interface.save_history(HISTORY_FILE) {
                    eprintln!("Could not save history file {}: {}", HISTORY_FILE, e);
                } else {
                    println!("History saved to {}", HISTORY_FILE);
                }
            }
            "quit" => break,
            "set" => {
                let d = parse_text("<input>", &line);
                interface.evaluate_directives(d);
            }
            _ => {
                match cli.cmd(&line) {
                    Err(e) => {
                        println!("{}", e.description());
                    }
                    Ok(msg) => {
                        println!("{}", msg);
                    }
                };
            }
        }
    }

    println!("Goodbye.");

    Ok(())
}

fn split_first_word(s: &str) -> (&str, &str) {
    let s = s.trim();

    match s.find(|ch: char| ch.is_whitespace()) {
        Some(pos) => (&s[..pos], s[pos..].trim_start()),
        None => (s, "")
    }
}

static DEMO_COMMANDS: &[(&str, &str)] = &[
    ("bind",             "Set bindings in inputrc format"),
    ("get",              "Print the value of a variable"),
    ("help",             "You're looking at it"),
    ("list-bindings",    "List bound sequences"),
    ("list-commands",    "List command names"),
    ("list-variables",   "List variables"),
    ("spawn-log-thread", "Spawns a thread that concurrently logs messages"),
    ("history",          "Print history"),
    ("save-history",     "Write history to file"),
    ("quit",             "Quit the demo"),
    ("set",              "Assign a value to a variable"),
    ("COUNT IN MEM",     "Count number of in-memory updates in selected orderbook"),
    ("COUNT ALL IN MEM", "Count number of in-memory updates"),
];

struct DemoCompleter;

impl<Term: Terminal> Completer<Term> for DemoCompleter {
    fn complete(&self, word: &str, prompter: &Prompter<Term>,
            start: usize, _end: usize) -> Option<Vec<Completion>> {
        let line = prompter.buffer();

        let mut words = line[..start].split_whitespace();

        match words.next() {
            // Complete command name
            None => {
                let mut compls = Vec::new();

                for &(cmd, _) in DEMO_COMMANDS {
                    if cmd.starts_with(word) {
                        compls.push(Completion::simple(cmd.to_owned()));
                    }
                }

                Some(compls)
            }
            // Complete command parameters
            Some("get") | Some("set") => {
                if words.count() == 0 {
                    let mut res = Vec::new();

                    for (name, _) in prompter.variables() {
                        if name.starts_with(word) {
                            res.push(Completion::simple(name.to_owned()));
                        }
                    }

                    Some(res)
                } else {
                    None
                }
            }
            _ => None
        }
    }
}