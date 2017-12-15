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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_bigram() {
        let a = vec![1,2,3];
        assert_eq!(bigram(&a), vec![(1,2), (2,3)]);
    }

}