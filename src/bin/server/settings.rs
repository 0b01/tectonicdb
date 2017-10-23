/// autoflush: boolean. Flush everything to disk at some interval.
/// dtf_folder: string. folder to save .dtf files
/// flush_interval: u32. flush at some regular interval.

#[derive(Clone, Debug)]
pub struct Settings {
    pub autoflush: bool,
    pub dtf_folder: String,
    pub flush_interval: u32,
    pub threads: usize,
}
