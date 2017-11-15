
/// fill digits 123 => 12300 etc..
/// 151044287500 => 1510442875000 
pub fn fill_digits(input: u64) -> u64 {
    let mut ret = input;
    while ret < 1_000_000_000_000  {
        ret *= 10;
    }
    ret
}

pub mod PriceHistogram {

    use histogram::Histogram;

    pub type Price = f64;

    pub struct PriceHistogram {
        hist: Histogram,
    }

    impl PriceHistogram {
        pub fn new(prices: &Vec<Price>) -> PriceHistogram{
            let mut histogram = Histogram::new();
            for &price in prices.iter() {
                let p_u64 = (price * 1_0000_0000. /*satoshi*/) as u64;
                let _ = histogram.increment(p_u64);
            }

            PriceHistogram {
                hist: histogram
            }
        }

        pub fn get_percentile(&self, middle_percent: f64) -> (Price, Price) {

            let high = 50. + middle_percent / 2.;
            let low = 50. - middle_percent / 2.;

            return (self.hist.percentile(low).unwrap() as f64 / 1_0000_0000.,
                    self.hist.percentile(high).unwrap() as f64/ 1_0000_0000.
                );
        }
    }

}


#[cfg(test)]
mod tests {

    use super::*;
    use super::PriceHistogram::{PriceHistogram, Price};
    static FNAME : &str = "test-data/bt_btcnav.dtf";

    #[test]
    fn test_histogram() {

        return;

        let records = super::super::decode(FNAME, Some(10000));
        let prices: Vec<Price> = records.into_iter()
                                    .map(|up| up.price as f64) 
                                    .collect();

        let hist = PriceHistogram::new(&prices);

        for i in 0..101 {
            let per = hist.get_percentile(i as f64);
            println!("{:?}", per);
        }
    }
}