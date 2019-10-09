// pub const INPUT_DIM: usize = 999;
// pub const TIME_STEP: usize = 999;
// pub const BATCH_SIZE: usize = 1;

pub struct Record {
    pub batches: Vec<Vec<Vec<f32>>>,
    pub batch_size: usize,
    pub time_step: usize,
    pub input_dim: usize,
}

impl Record {
    pub fn new(batch_size: usize, time_step: usize, input_dim: usize) -> Record {
        let batches = vec![vec![vec![0.; input_dim]; time_step]; batch_size];

        Record {
            batches,
            batch_size,
            time_step,
            input_dim,
        }
    }

    pub fn merge_from(&mut self, other: Record) {
        // dimensions need to be equal
        assert_eq!(self.time_step, other.time_step);
        assert_eq!(self.input_dim, other.input_dim);

        // extend vector
        self.batches.extend(other.batches);
        self.batch_size += other.batch_size;
    }
}
