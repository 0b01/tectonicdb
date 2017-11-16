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
        cache: Vec<Option<f64>>
    }

    impl PriceHistogram {
        pub fn new(prices: &Vec<Price>) -> PriceHistogram{
            let mut histogram = Histogram::new();
            for &price in prices.iter() {
                let p_u64 = (price * 1_0000_0000. /*satoshi*/) as u64;
                let _ = histogram.increment(p_u64);
            }

            PriceHistogram {
                hist: histogram,
                cache: vec![None; 101],
            }
        }

        pub fn get_percentile(&mut self, middle_percent: u8) -> (Price, Price) {

            let high = 50. + middle_percent as f64 / 2.;
            let low = 50. - middle_percent  as f64 / 2.;
            
            let cache = |c: &mut Vec<Option<f64>>, k: usize, v:f64| { c[k] = Some(v); };

            let (low_price, high_price) = {
                let low_p  = self.cache.get(low  as usize).map(|&s| s).unwrap();
                let high_p = self.cache.get(high as usize).map(|&s| s).unwrap();
                (low_p, high_p)
            };

            match (low_price, high_price) {
                (Some(l), Some(h)) => (l, h),
                (_, Some(h)) => {

                    let lowval = self.hist.percentile(low).unwrap() as f64 / 1_0000_0000.;
                    cache(&mut self.cache, low as usize, low);

                    (lowval, h)
                },
                (Some(l), _) => {

                    let highval = self.hist.percentile(low).unwrap() as f64 / 1_0000_0000.;
                    cache(&mut self.cache, high as usize, highval);

                    (l, highval)
                },
                (None, None) => {
                    let highval = self.hist.percentile(high).unwrap() as f64 / 1_0000_0000.;
                    cache(&mut self.cache, high as usize, highval);
                    let lowval = self.hist.percentile(low).unwrap() as f64 / 1_0000_0000.;
                    cache(&mut self.cache, low as usize, lowval);
                    (lowval, highval)
                }
            }
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

        // this test is pretty hard to fail...
        // return;

        let records = super::super::decode(FNAME, Some(10000));
        let prices: Vec<Price> = records.into_iter()
                                    .map(|up| up.price as f64) 
                                    .collect();

        let mut hist = PriceHistogram::new(&prices);

        use std::time::Instant;

        for i in 1..10 {
            let now = Instant::now();
            {
                for i in 0..101 {
                    let per = hist.get_percentile(i);
                }
            }
            let elapsed = now.elapsed();
            let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
            println!("Seconds: {}", sec);
        }
    }
}