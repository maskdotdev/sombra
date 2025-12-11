//! Storage utility functions.

use std::cmp::Ordering;

/// Checks if a slice is sorted in ascending order using a comparison function.
///
/// Returns `true` if the slice is sorted (i.e., for all adjacent pairs `(a, b)`,
/// `cmp(a, b)` does not return `Ordering::Greater`).
///
/// This is useful as a guard before calling `sort_by` to avoid O(n log n) sorting
/// when the data is already in order (common in bulk ingest scenarios).
#[inline]
pub fn is_sorted_by<T, F>(slice: &[T], mut cmp: F) -> bool
where
    F: FnMut(&T, &T) -> Ordering,
{
    slice
        .windows(2)
        .all(|w| cmp(&w[0], &w[1]) != Ordering::Greater)
}

/// Checks if a slice is sorted in ascending order.
///
/// This is a convenience wrapper around [`is_sorted_by`] for types that implement `Ord`.
#[inline]
pub fn is_sorted<T: Ord>(slice: &[T]) -> bool {
    is_sorted_by(slice, |a, b| a.cmp(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_sorted_empty() {
        let empty: &[i32] = &[];
        assert!(is_sorted(empty));
    }

    #[test]
    fn test_is_sorted_single() {
        assert!(is_sorted(&[42]));
    }

    #[test]
    fn test_is_sorted_ascending() {
        assert!(is_sorted(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_is_sorted_with_duplicates() {
        assert!(is_sorted(&[1, 2, 2, 3, 3, 3, 4]));
    }

    #[test]
    fn test_is_sorted_descending() {
        assert!(!is_sorted(&[5, 4, 3, 2, 1]));
    }

    #[test]
    fn test_is_sorted_unsorted() {
        assert!(!is_sorted(&[1, 3, 2, 4]));
    }

    #[test]
    fn test_is_sorted_by_custom() {
        let data = vec![(1, "a"), (2, "b"), (3, "c")];
        assert!(is_sorted_by(&data, |a, b| a.0.cmp(&b.0)));
    }

    #[test]
    fn test_is_sorted_by_reverse() {
        let data = vec![5, 4, 3, 2, 1];
        // Sorted in descending order
        assert!(is_sorted_by(&data, |a, b| b.cmp(a)));
    }
}
