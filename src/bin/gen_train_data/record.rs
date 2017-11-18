pub const INPUT_DIM: usize = 6;
pub const TIME_STEP: usize = 5;
pub const BATCH_SIZE: usize = 4;

pub type Record = [[[ f32 ; INPUT_DIM]; TIME_STEP]; BATCH_SIZE];