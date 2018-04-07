//! A circular buffer-like queue.
//!
//! The `CircularQueue<T>` is created with a set capacity, then items are pushed in. When the queue
//! runs out of capacity, newer items start overwriting the old ones, starting from the oldest.
//!
//! There's a built-in iterator that goes from the newest items to the oldest ones.
//!
//! # Examples
//!
//! ```
//! use circular_queue::CircularQueue;
//!
//! let mut queue = CircularQueue::with_capacity(3);
//! queue.push(1);
//! queue.push(2);
//! queue.push(3);
//! queue.push(4);
//!
//! assert_eq!(queue.len(), 3);
//!
//! let mut iter = queue.iter();
//!
//! assert_eq!(iter.next(), Some(&4));
//! assert_eq!(iter.next(), Some(&3));
//! assert_eq!(iter.next(), Some(&2));
//! ```

use std::ptr;
use std::iter::{Chain, Rev};
use std::slice::{Iter as SliceIter, IterMut as SliceIterMut};

/// A circular buffer-like queue.
#[derive(Clone, Debug)]
pub struct CircularQueue<T> {
    data: Vec<T>,
    // Using our own capacity instead of the one stored in Vec to ensure consistent behavior with
    // zero-sized types.
    capacity: usize,
    insertion_index: usize,
    reverse_idx: usize,
}

/// An iterator over `CircularQueue<T>`.
pub type Iter<'a, T> = Chain<Rev<SliceIter<'a, T>>, Rev<SliceIter<'a, T>>>;

/// A mutable iterator over `CircularQueue<T>`.
pub type IterMut<'a, T> = Chain<Rev<SliceIterMut<'a, T>>, Rev<SliceIterMut<'a, T>>>;

impl<T> CircularQueue<T> {
    /// Constructs a new, empty `CircularQueue<T>` with the requested capacity.
    ///
    /// # Panics
    ///
    /// Panics if the requested capacity is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue: CircularQueue<i32> = CircularQueue::with_capacity(5);
    /// ```
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            panic!("capacity must be greater than 0");
        }

        Self {
            data: Vec::with_capacity(capacity),
            capacity,
            insertion_index: 0,
            reverse_idx: 0,
        }
    }

    /// Returns the current number of elements in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(5);
    /// queue.push(1);
    /// queue.push(2);
    /// queue.push(3);
    ///
    /// assert_eq!(queue.len(), 3);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the queue contains no elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(5);
    /// assert!(queue.is_empty());
    ///
    /// queue.push(1);
    /// assert!(!queue.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the capacity of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let queue: CircularQueue<i32> = CircularQueue::with_capacity(5);
    /// assert_eq!(queue.capacity(), 5);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Clears the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(5);
    /// queue.push(1);
    /// queue.push(2);
    /// queue.push(3);
    ///
    /// queue.clear();
    /// assert_eq!(queue.len(), 0);
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
        self.insertion_index = 0;
    }

    /// Pushes a new element into the queue.
    ///
    /// Once the capacity is reached, pushing new items will overwrite old ones.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(3);
    /// queue.push(1);
    /// queue.push(2);
    /// queue.push(3);
    /// queue.push(4);
    ///
    /// assert_eq!(queue.len(), 3);
    ///
    /// let mut iter = queue.iter();
    ///
    /// assert_eq!(iter.next(), Some(&4));
    /// assert_eq!(iter.next(), Some(&3));
    /// assert_eq!(iter.next(), Some(&2));
    /// ```
    pub fn push(&mut self, x: T) {
        if self.data.len() < self.capacity() {
            self.data.push(x);
        } else {
            self.data[self.insertion_index] = x;
        }
        if self.reverse_idx > 0 {
            self.reverse_idx -= 1;
        }
        self.insertion_index = (self.insertion_index + 1) % self.capacity();
    }

    /// Pops the elements from the queue(LIFO).
    ///
    /// Pops at max self.capacity number of items.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(3);
    /// queue.push(1);
    /// queue.push(2);
    /// queue.push(3);
    /// queue.push(4);
    ///
    /// assert_eq!(queue.len(), 3);
    ///
    /// assert_eq!(queue.pop(), Some(4));
    /// assert_eq!(queue.pop(), Some(3));
    /// assert_eq!(queue.pop(), Some(2));
    /// assert_eq!(queue.pop(), None);
    /// 
    /// ```
    pub fn pop(&mut self) -> Option<T> {
        if self.data.len() == 0 {
            None
        } else if self.data.len() < self.capacity() {
            self.data.pop()
        } else if self.reverse_idx == self.capacity() {
            None
        } else {
            self.reverse_idx += 1;
            self.insertion_index += self.capacity - 1;
            self.insertion_index %= self.capacity;
            unsafe {
                Some(ptr::read(self.data.get_unchecked(self.insertion_index)))
            }
        }
    }

    /// Returns an iterator over the queue's contents.
    ///
    /// The iterator goes from the most recently pushed items to the oldest ones.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(3);
    /// queue.push(1);
    /// queue.push(2);
    /// queue.push(3);
    /// queue.push(4);
    ///
    /// let mut iter = queue.iter();
    ///
    /// assert_eq!(iter.next(), Some(&4));
    /// assert_eq!(iter.next(), Some(&3));
    /// assert_eq!(iter.next(), Some(&2));
    /// ```
    #[inline]
    pub fn iter(&self) -> Iter<T> {
        let (a, b) = self.data.split_at(self.insertion_index);
        a.iter().rev().chain(b.iter().rev())
    }

    /// Returns a mutable iterator over the queue's contents.
    ///
    /// The iterator goes from the most recently pushed items to the oldest ones.
    ///
    /// # Examples
    ///
    /// ```
    /// use circular_queue::CircularQueue;
    ///
    /// let mut queue = CircularQueue::with_capacity(3);
    /// queue.push(1);
    /// queue.push(2);
    /// queue.push(3);
    /// queue.push(4);
    ///
    /// let mut iter = queue.iter_mut();
    ///
    /// assert_eq!(iter.next(), Some(&mut 4));
    /// assert_eq!(iter.next(), Some(&mut 3));
    /// assert_eq!(iter.next(), Some(&mut 2));
    /// ```
    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<T> {
        let (a, b) = self.data.split_at_mut(self.insertion_index);
        a.iter_mut().rev().chain(b.iter_mut().rev())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn zero_capacity() {
        let _ = CircularQueue::<i32>::with_capacity(0);
    }

    #[test]
    fn empty_queue() {
        let q = CircularQueue::<i32>::with_capacity(5);

        assert_eq!(q.iter().next(), None);
    }

    #[test]
    fn partially_full_queue() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);

        assert_eq!(q.len(), 3);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [3, 2, 1]);
    }

    #[test]
    fn full_queue() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);

        assert_eq!(q.len(), 5);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [5, 4, 3, 2, 1]);
    }

    #[test]
    fn over_full_queue() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        assert_eq!(q.len(), 5);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [7, 6, 5, 4, 3]);
    }

    #[test]
    fn clear() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        q.clear();

        assert_eq!(q.len(), 0);
        assert_eq!(q.iter().next(), None);

        q.push(1);
        q.push(2);
        q.push(3);

        assert_eq!(q.len(), 3);

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [3, 2, 1]);
    }

    #[test]
    fn popping_then_pushing() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        let res = q.pop();
        assert_eq!(res, Some(7));
        let res = q.pop();
        assert_eq!(res, Some(6));
        let res = q.pop();
        assert_eq!(res, Some(5));
        let res = q.pop();
        assert_eq!(res, Some(4));
        let res = q.pop();
        assert_eq!(res, Some(3));
        let res = q.pop();
        assert_eq!(res, None);

        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        let res = q.pop();
        assert_eq!(res, Some(7));
        let res = q.pop();
        assert_eq!(res, Some(6));
        let res = q.pop();
        assert_eq!(res, Some(5));
        let res = q.pop();
        assert_eq!(res, Some(4));
        let res = q.pop();
        assert_eq!(res, Some(3));
        let res = q.pop();
        assert_eq!(res, None);

    }

    #[test]
    fn popping() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        let res = q.pop();
        assert_eq!(res, Some(7));
        let res = q.pop();
        assert_eq!(res, Some(6));
        let res = q.pop();
        assert_eq!(res, Some(5));
        let res = q.pop();
        assert_eq!(res, Some(4));
        let res = q.pop();
        assert_eq!(res, Some(3));
        let res = q.pop();
        assert_eq!(res, None);
    }

    #[test]
    fn mutable_iterator() {
        let mut q = CircularQueue::with_capacity(5);
        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);
        q.push(5);
        q.push(6);
        q.push(7);

        for x in q.iter_mut() {
            *x *= 2;
        }

        let res: Vec<_> = q.iter().map(|&x| x).collect();
        assert_eq!(res, [14, 12, 10, 8, 6]);
    }

    #[test]
    fn zero_sized() {
        let mut q = CircularQueue::with_capacity(3);
        assert_eq!(q.capacity(), 3);

        q.push(());
        q.push(());
        q.push(());
        q.push(());

        assert_eq!(q.len(), 3);

        let mut iter = q.iter();
        assert_eq!(iter.next(), Some(&()));
        assert_eq!(iter.next(), Some(&()));
        assert_eq!(iter.next(), Some(&()));
        assert_eq!(iter.next(), None);
    }
}
