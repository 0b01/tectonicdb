/// fill digits 123 => 12300 etc..
/// 151044287500 => 1510442875000 
pub fn fill_digits(input: u64) -> u64 {
    let mut ret = input;
    while ret < 1_000_000_000_000  {
        ret *= 10;
    }
    ret
}

/// Returns bigram
///     bigram(&[1,2,3]) -> [(1,2), (2,3)]
pub fn bigram<T: Copy>(a: &[T]) -> Vec<(T,T)> {
    a.into_iter()
        .map(|&t| t)
        .zip(a[1..].into_iter().map(|&t| t))
        .collect::<Vec<(_, _)>>()
}

pub mod price_histogram {

    use std::mem;
    use std::cmp::Ordering::{self, Equal, Greater, Less};
    use std::collections::HashMap;
    use super::super::utils::bigram;
    use super::super::Update;
    use super::fill_digits;

    pub type Price = f64;
    pub type Count = usize;

    #[derive(Debug)]    
    pub struct Histogram {
        pub bins: Option<Vec<Count>>,
        pub boundaries: Vec<Price>,
        pub boundary2idx: HashMap<u64, usize>,
        pub cached_bigram: Vec<(f64,f64)>
    }

    impl Histogram {

        pub fn new(prices: &[Price], bin_count: Count, m: f64) -> Histogram {
            let filtered = reject_outliers(prices, m);
            build_histogram(filtered, bin_count)
        }

        pub fn to_bin(&self, price : Price) -> Option<Price> {
            let cb = &self.cached_bigram;
            for &(s, b) in cb.iter() {
                if (s == price) || (b > price && price > s) {
                    return Some(s);
                }
            }
            return None;
        }

        fn new_boundaries(min_ts: u64, max_ts: u64, step_bins: usize) -> Histogram {
            let bucket_size = (max_ts - min_ts) / ((step_bins - 1) as u64);
            let mut boundaries = vec![];

            // build boundary lookup table
            let mut lookup_table = HashMap::new();
            for i in 0..step_bins {
                let boundary = (min_ts + (i as u64) * bucket_size) as f64;
                boundaries.push(boundary);
                lookup_table.insert(boundary.to_bits(), i);
            }

            // cache bigram
            let cached_bigram = bigram(&boundaries);

            Histogram { 
                bins: None, 
                boundaries, 
                boundary2idx: lookup_table,
                cached_bigram: cached_bigram
            }
        }

        /// get spatial temporal histograms
        /// m is value of z-score cutoff
        pub fn from(ups: &[Update], step_bins: Count, tick_bins: Count, m: f64)
              -> (Histogram, Histogram) {
            // build price histogram
            let prices = ups.iter().map(|up| up.price as f64).collect::<Vec<f64>>();
            let price_hist = Histogram::new(&prices, tick_bins, m);

            // build time step histogram
            let min_ts = fill_digits(ups.iter().next().unwrap().ts) / 1000;
            let max_ts = fill_digits(ups.iter().next_back().unwrap().ts) / 1000;
            let step_hist = Histogram::new_boundaries(min_ts, max_ts, step_bins);

            (price_hist, step_hist)
        }

        pub fn index(&self, price: Price) -> usize {
            *self.boundary2idx.get(&price.to_bits()).unwrap()
        }
    }

    pub fn reject_outliers(prices: &[Price], m: f64) -> Vec<Price> {
        let median = (*prices).median();

        // println!("len before: {}", prices.len());
        // let m = 2.;
        let d = prices.iter().map(|p|{
            let v = p - median;
            if v > 0. { v } else { -v }
        }).collect::<Vec<f64>>();
        let mdev = d.median();
        let s = d.iter().map(|a| {
            if mdev > 0. {a / mdev} else {0.}
        }).collect::<Vec<f64>>();
        let filtered = prices.iter().enumerate()
                            .filter(|&(i, _p)| s[i] < m)
                            .map(|(_i, &p)| p)
                            .collect::<Vec<f64>>();

        // println!("len after: {}", filtered.len());

        filtered
    }

    pub fn build_histogram(filtered_vals: Vec<Price>, bin_count: Count) -> Histogram {
        let max = &filtered_vals.max();
        let min = &filtered_vals.min();
        let bucket_size = (max - min) / ((bin_count - 1) as f64);

        let mut bins = vec![0; bin_count as usize];
        for price in filtered_vals.iter() {
            let mut bucket_index = 0;
            if bucket_size > 0.0 {
                bucket_index = ((price - min) / bucket_size) as usize;
                if bucket_index == bin_count {
                    bucket_index -= 1;
                }
            }
            bins[bucket_index] += 1;
        }

        let mut boundaries = vec![];
        let mut lookup_table = HashMap::new();
        for i in 0..bin_count {
            let boundary = min + i as f64 * bucket_size;
            boundaries.push(boundary);
            lookup_table.insert(boundary.to_bits(), i);
        }


        // cache bigram
        let cached_bigram = bigram(&boundaries);
        

        Histogram {
            bins: Some(bins),
            boundaries,
            boundary2idx: lookup_table,
            cached_bigram
        }

    }

    /// Trait that provides simple descriptive statistics on a univariate set of numeric samples.
    pub trait Stats {
        /// Sum of the samples.
        ///
        /// Note: this method sacrifices performance at the altar of accuracy
        /// Depends on IEEE-754 arithmetic guarantees. See proof of correctness at:
        /// ["Adaptive Precision Floating-Point Arithmetic and Fast Robust Geometric
        /// Predicates"][paper]
        ///
        /// [paper]: http://www.cs.cmu.edu/~quake-papers/robust-arithmetic.ps
        fn sum(&self) -> f64;

        /// Minimum value of the samples.
        fn min(&self) -> f64;

        /// Maximum value of the samples.
        fn max(&self) -> f64;

        /// Arithmetic mean (average) of the samples: sum divided by sample-count.
        ///
        /// See: https://en.wikipedia.org/wiki/Arithmetic_mean
        fn mean(&self) -> f64;

        /// Median of the samples: value separating the lower half of the samples from the higher half.
        /// Equal to `self.percentile(50.0)`.
        ///
        /// See: https://en.wikipedia.org/wiki/Median
        fn median(&self) -> f64;

        /// Variance of the samples: bias-corrected mean of the squares of the differences of each
        /// sample from the sample mean. Note that this calculates the _sample variance_ rather than the
        /// population variance, which is assumed to be unknown. It therefore corrects the `(n-1)/n`
        /// bias that would appear if we calculated a population variance, by dividing by `(n-1)` rather
        /// than `n`.
        ///
        /// See: https://en.wikipedia.org/wiki/Variance
        fn var(&self) -> f64;

        /// Standard deviation: the square root of the sample variance.
        ///
        /// Note: this is not a robust statistic for non-normal distributions. Prefer the
        /// `median_abs_dev` for unknown distributions.
        ///
        /// See: https://en.wikipedia.org/wiki/Standard_deviation
        fn std_dev(&self) -> f64;

        /// Standard deviation as a percent of the mean value. See `std_dev` and `mean`.
        ///
        /// Note: this is not a robust statistic for non-normal distributions. Prefer the
        /// `median_abs_dev_pct` for unknown distributions.
        fn std_dev_pct(&self) -> f64;

        /// Scaled median of the absolute deviations of each sample from the sample median. This is a
        /// robust (distribution-agnostic) estimator of sample variability. Use this in preference to
        /// `std_dev` if you cannot assume your sample is normally distributed. Note that this is scaled
        /// by the constant `1.4826` to allow its use as a consistent estimator for the standard
        /// deviation.
        ///
        /// See: http://en.wikipedia.org/wiki/Median_absolute_deviation
        fn median_abs_dev(&self) -> f64;

        /// Median absolute deviation as a percent of the median. See `median_abs_dev` and `median`.
        fn median_abs_dev_pct(&self) -> f64;

        /// Percentile: the value below which `pct` percent of the values in `self` fall. For example,
        /// percentile(95.0) will return the value `v` such that 95% of the samples `s` in `self`
        /// satisfy `s <= v`.
        ///
        /// Calculated by linear interpolation between closest ranks.
        ///
        /// See: http://en.wikipedia.org/wiki/Percentile
        fn percentile(&self, pct: f64) -> f64;

        /// Quartiles of the sample: three values that divide the sample into four equal groups, each
        /// with 1/4 of the data. The middle value is the median. See `median` and `percentile`. This
        /// function may calculate the 3 quartiles more efficiently than 3 calls to `percentile`, but
        /// is otherwise equivalent.
        ///
        /// See also: https://en.wikipedia.org/wiki/Quartile
        fn quartiles(&self) -> (f64, f64, f64);

        /// Inter-quartile range: the difference between the 25th percentile (1st quartile) and the 75th
        /// percentile (3rd quartile). See `quartiles`.
        ///
        /// See also: https://en.wikipedia.org/wiki/Interquartile_range
        fn iqr(&self) -> f64;
    }

    impl Stats for [f64] {
        // FIXME #11059 handle NaN, inf and overflow
        fn sum(&self) -> f64 {
            let mut partials = vec![];

            for &x in self {
                let mut x = x;
                let mut j = 0;
                // This inner loop applies `hi`/`lo` summation to each
                // partial so that the list of partial sums remains exact.
                for i in 0..partials.len() {
                    let mut y: f64 = partials[i];
                    if x.abs() < y.abs() {
                        mem::swap(&mut x, &mut y);
                    }
                    // Rounded `x+y` is stored in `hi` with round-off stored in
                    // `lo`. Together `hi+lo` are exactly equal to `x+y`.
                    let hi = x + y;
                    let lo = y - (hi - x);
                    if lo != 0.0 {
                        partials[j] = lo;
                        j += 1;
                    }
                    x = hi;
                }
                if j >= partials.len() {
                    partials.push(x);
                } else {
                    partials[j] = x;
                    partials.truncate(j + 1);
                }
            }
            let zero: f64 = 0.0;
            partials.iter().fold(zero, |p, q| p + *q)
        }

        fn min(&self) -> f64 {
            assert!(!self.is_empty());
            self.iter().fold(self[0], |p, q| p.min(*q))
        }

        fn max(&self) -> f64 {
            assert!(!self.is_empty());
            self.iter().fold(self[0], |p, q| p.max(*q))
        }

        fn mean(&self) -> f64 {
            assert!(!self.is_empty());
            self.sum() / (self.len() as f64)
        }

        fn median(&self) -> f64 {
            self.percentile(50 as f64)
        }

        fn var(&self) -> f64 {
            if self.len() < 2 {
                0.0
            } else {
                let mean = self.mean();
                let mut v: f64 = 0.0;
                for s in self {
                    let x = *s - mean;
                    v = v + x * x;
                }
                // NB: this is _supposed to be_ len-1, not len. If you
                // change it back to len, you will be calculating a
                // population variance, not a sample variance.
                let denom = (self.len() - 1) as f64;
                v / denom
            }
        }

        fn std_dev(&self) -> f64 {
            self.var().sqrt()
        }

        fn std_dev_pct(&self) -> f64 {
            let hundred = 100 as f64;
            (self.std_dev() / self.mean()) * hundred
        }

        fn median_abs_dev(&self) -> f64 {
            let med = self.median();
            let abs_devs: Vec<f64> = self.iter().map(|&v| (med - v).abs()).collect();
            // This constant is derived by smarter statistics brains than me, but it is
            // consistent with how R and other packages treat the MAD.
            let number = 1.4826;
            abs_devs.median() * number
        }

        fn median_abs_dev_pct(&self) -> f64 {
            let hundred = 100 as f64;
            (self.median_abs_dev() / self.median()) * hundred
        }

        fn percentile(&self, pct: f64) -> f64 {
            let mut tmp = self.to_vec();
            local_sort(&mut tmp);
            percentile_of_sorted(&tmp, pct)
        }

        fn quartiles(&self) -> (f64, f64, f64) {
            let mut tmp = self.to_vec();
            local_sort(&mut tmp);
            let first = 25f64;
            let a = percentile_of_sorted(&tmp, first);
            let second = 50f64;
            let b = percentile_of_sorted(&tmp, second);
            let third = 75f64;
            let c = percentile_of_sorted(&tmp, third);
            (a, b, c)
        }

        fn iqr(&self) -> f64 {
            let (a, _, c) = self.quartiles();
            c - a
        }
    }

    // Helper function: extract a value representing the `pct` percentile of a sorted sample-set, using
    // linear interpolation. If samples are not sorted, return nonsensical value.
    fn percentile_of_sorted(sorted_samples: &[f64], pct: f64) -> f64 {
        assert!(!sorted_samples.is_empty());
        if sorted_samples.len() == 1 {
            return sorted_samples[0];
        }
        let zero: f64 = 0.0;
        assert!(zero <= pct);
        let hundred = 100f64;
        assert!(pct <= hundred);
        if pct == hundred {
            return sorted_samples[sorted_samples.len() - 1];
        }
        let length = (sorted_samples.len() - 1) as f64;
        let rank = (pct / hundred) * length;
        let lrank = rank.floor();
        let d = rank - lrank;
        let n = lrank as usize;
        let lo = sorted_samples[n];
        let hi = sorted_samples[n + 1];
        lo + (hi - lo) * d
    }

    fn local_sort(v: &mut [f64]) {
        v.sort_by(|x: &f64, y: &f64| local_cmp(*x, *y));
    }

    fn local_cmp(x: f64, y: f64) -> Ordering {
        // arbitrarily decide that NaNs are larger than everything.
        if y.is_nan() {
            Less
        } else if x.is_nan() {
            Greater
        } else if x < y {
            Less
        } else if x == y {
            Equal
        } else {
            Greater
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use super::price_histogram::{Histogram, Price};
    static FNAME : &str = "test-data/bt_btcnav.dtf";
    use std::collections::HashMap;

    #[test]
    fn test_histogram() {
        let records = super::super::decode(FNAME, Some(10000));
        let prices: Vec<Price> = records.into_iter()
                                    .map(|up| up.price as f64) 
                                    .collect();

        let mut hist = Histogram::new(&prices, 100, 2.);

        // println!("{:?}", hist.bins);
        

        // use std::time::Instant;

        // for i in 1..10 {
        //     let now = Instant::now();
        //     {
        //         for i in 0..101 {
        //             let per = hist.get_percentile(i);
        //         }
        //     }
        //     let elapsed = now.elapsed();
        //     let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        //     println!("Seconds: {}", sec);
        // }
    }

    #[test]
    fn test_bigram() {
        let a = vec![1,2,3];
        assert_eq!(bigram(&a), vec![(1,2), (2,3)]);
    }

    #[test]
    fn test_epoch_histogram() {
        let step_bins = 10;
        let min_ts = 1_000;
        let max_ts = 10_000;
        let bucket_size = (max_ts - min_ts) / (step_bins as u64 - 1);
        let mut boundaries = vec![];
        let mut boundary2idx = HashMap::new();
        for i in 0..step_bins {
            let boundary = min_ts as f64 + i as f64 * bucket_size as f64;
            boundaries.push(boundary);
            boundary2idx.insert(boundary.to_bits(), i);
        }

        let cached_bigram = bigram(&boundaries);

        let step_hist = Histogram { 
            bins: None,
            boundaries, 
            boundary2idx, 
            cached_bigram
        };

        assert_eq!(step_hist.boundaries.len(), step_bins as usize);
        for i in min_ts..max_ts {
            assert_eq!(Some((i / 1000 * 1000) as f64), step_hist.to_bin(i as f64));
        }
    }
}