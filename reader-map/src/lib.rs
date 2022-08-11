//! A lock-free, eventually consistent, concurrent multi-value map.
//!
//! This map implementation allows reads and writes to execute entirely in parallel, with no
//! implicit synchronization overhead. Reads never take locks on their critical path, and neither
//! do writes assuming there is a single writer (multi-writer is possible using a `Mutex`), which
//! significantly improves performance under contention. See the [`left-right` crate](left_right)
//! for details on the underlying concurrency primitive.
//!
//! The trade-off exposed by this type is one of eventual consistency: writes are not visible to
//! readers except following explicit synchronization. Specifically, readers only see the
//! operations that preceeded the last call to `WriteHandle::refresh` by a writer. This lets
//! writers decide how stale they are willing to let reads get. They can refresh the map after
//! every write to emulate a regular concurrent `HashMap`, or they can refresh only occasionally to
//! reduce the synchronization overhead at the cost of stale reads.
//!
//! For read-heavy workloads, the scheme used by this module is particularly useful. Writers can
//! afford to refresh after every write, which provides up-to-date reads, and readers remain fast
//! as they do not need to ever take locks.
//!
//! The map is multi-value, meaning that every key maps to a *collection* of values. This
//! introduces some memory cost by adding a layer of indirection through a `Vec` for each value,
//! but enables more advanced use. This choice was made as it would not be possible to emulate such
//! functionality on top of the semantics of this map (think about it -- what would the operational
//! log contain?).
//!
//! To faciliate more advanced use-cases, each of the two maps also carry some customizeable
//! meta-information. The writers may update this at will, and when a refresh happens, the current
//! meta will also be made visible to readers. This could be useful, for example, to indicate what
//! time the refresh happened.
//!
//! # Examples
//!
//! Single-reader, single-writer
//!
//! ```
//! // new will use the default HashMap hasher, and a meta of ()
//! // note that we get separate read and write handles
//! // the read handle can be cloned to have more readers
//! let (mut book_reviews_w, book_reviews_r) = reader_map::new();
//!
//! // review some books.
//! book_reviews_w.insert("Adventures of Huckleberry Finn", "My favorite book.");
//! book_reviews_w.insert("Grimms' Fairy Tales", "Masterpiece.");
//! book_reviews_w.insert("Pride and Prejudice", "Very enjoyable.");
//! book_reviews_w.insert("The Adventures of Sherlock Holmes", "Eye lyked it alot.");
//!
//! // at this point, reads from book_reviews_r will not see any of the reviews!
//! assert_eq!(book_reviews_r.len(), 0);
//! // we need to refresh first to make the writes visible
//! book_reviews_w.publish();
//! assert_eq!(book_reviews_r.len(), 4);
//! // reads will now return Ok() because the map has been initialized
//! assert_eq!(
//!     book_reviews_r
//!         .get("Grimms' Fairy Tales")
//!         .unwrap()
//!         .map(|rs| rs.len()),
//!     Some(1)
//! );
//!
//! // remember, this is a multi-value map, so we can have many reviews
//! book_reviews_w.insert("Grimms' Fairy Tales", "Eh, the title seemed weird.");
//! book_reviews_w.insert("Pride and Prejudice", "Too many words.");
//!
//! // but again, new writes are not yet visible
//! assert_eq!(
//!     book_reviews_r
//!         .get("Grimms' Fairy Tales")
//!         .unwrap()
//!         .map(|rs| rs.len()),
//!     Some(1)
//! );
//!
//! // we need to refresh first
//! book_reviews_w.publish();
//! assert_eq!(
//!     book_reviews_r
//!         .get("Grimms' Fairy Tales")
//!         .unwrap()
//!         .map(|rs| rs.len()),
//!     Some(2)
//! );
//!
//! // oops, this review has a lot of spelling mistakes, let's delete it.
//! // remove_entry deletes *all* reviews (though in this case, just one)
//! book_reviews_w.remove_entry("The Adventures of Sherlock Holmes");
//! // but again, it's not visible to readers until we refresh
//! assert_eq!(
//!     book_reviews_r
//!         .get("The Adventures of Sherlock Holmes")
//!         .unwrap()
//!         .map(|rs| rs.len()),
//!     Some(1)
//! );
//! book_reviews_w.publish();
//! assert_eq!(
//!     book_reviews_r
//!         .get("The Adventures of Sherlock Holmes")
//!         .unwrap()
//!         .map(|rs| rs.len()),
//!     None
//! );
//!
//! // look up the values associated with some keys.
//! let to_find = ["Pride and Prejudice", "Alice's Adventure in Wonderland"];
//! for book in &to_find {
//!     if let Some(reviews) = book_reviews_r.get(book).unwrap() {
//!         for review in &*reviews {
//!             println!("{}: {}", book, review);
//!         }
//!     } else {
//!         println!("{} is unreviewed.", book);
//!     }
//! }
//!
//! // iterate over everything.
//! for (book, reviews) in &book_reviews_r.enter().unwrap() {
//!     for review in reviews {
//!         println!("{}: \"{}\"", book, review);
//!     }
//! }
//! ```
//!
//! Reads from multiple threads are possible by cloning the `ReadHandle`.
//!
//! ```
//! use std::thread;
//! let (mut book_reviews_w, book_reviews_r) = reader_map::new();
//!
//! // start some readers
//! let readers: Vec<_> = (0..4)
//!     .map(|_| {
//!         let r = book_reviews_r.clone();
//!         thread::spawn(move || {
//!             loop {
//!                 let l = r.len();
//!                 if l == 0 {
//!                     thread::yield_now();
//!                 } else {
//!                     // the reader will either see all the reviews,
//!                     // or none of them, since refresh() is atomic.
//!                     assert_eq!(l, 4);
//!                     break;
//!                 }
//!             }
//!         })
//!     })
//!     .collect();
//!
//! // do some writes
//! book_reviews_w.insert("Adventures of Huckleberry Finn", "My favorite book.");
//! book_reviews_w.insert("Grimms' Fairy Tales", "Masterpiece.");
//! book_reviews_w.insert("Pride and Prejudice", "Very enjoyable.");
//! book_reviews_w.insert("The Adventures of Sherlock Holmes", "Eye lyked it alot.");
//! // expose the writes
//! book_reviews_w.publish();
//!
//! // you can read through the write handle
//! assert_eq!(book_reviews_w.len(), 4);
//!
//! // the original read handle still works too
//! assert_eq!(book_reviews_r.len(), 4);
//!
//! // all the threads should eventually see .len() == 4
//! for r in readers.into_iter() {
//!     assert!(r.join().is_ok());
//! }
//! ```
//!
//! If multiple writers are needed, the `WriteHandle` must be protected by a `Mutex`.
//!
//! ```
//! use std::sync::{Arc, Mutex};
//! use std::thread;
//! let (mut book_reviews_w, book_reviews_r) = reader_map::new();
//!
//! // start some writers.
//! // since reader_map does not support concurrent writes, we need
//! // to protect the write handle by a mutex.
//! let w = Arc::new(Mutex::new(book_reviews_w));
//! let writers: Vec<_> = (0..4)
//!     .map(|i| {
//!         let w = w.clone();
//!         thread::spawn(move || {
//!             let mut w = w.lock().unwrap();
//!             w.insert(i, true);
//!             w.publish();
//!         })
//!     })
//!     .collect();
//!
//! // eventually we should see all the writes
//! while book_reviews_r.len() < 4 {
//!     thread::yield_now();
//! }
//!
//! // all the threads should eventually finish writing
//! for w in writers.into_iter() {
//!     assert!(w.join().is_ok());
//! }
//! ```
//!
//! [`ReadHandle`] is not `Sync` as sharing a single instance amongst threads would introduce a
//! significant performance bottleneck. A fresh `ReadHandle` needs to be created for each thread
//! by cloning a [`ReadHandle`]. For further information, see [`left_right::ReadHandle`].
//!
//! # Implementation
//!
//! Under the hood, the map is implemented using two identical collections and some magic. Take a
//! look at [`left-right`](left_right) for a much more in-depth discussion. Since the implementation
//! uses regular `HashMap`s under the hood, table resizing is fully supported. It does, however,
//! also mean that the memory usage of this implementation is approximately twice that of a regular
//! collection.
//!
//! # Value storage
//!
//! The values for each key in the map are stored in [`refs::Values`], which is internally is a
//! sorted vector of values.
#![warn(
    missing_docs,
    rust_2018_idioms,
    missing_debug_implementations,
    rustdoc::broken_intra_doc_links
)]
#![allow(clippy::type_complexity)]
#![feature(btree_drain_filter)]

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};

pub use eviction::EvictionStrategy;
use partial_map::InsertionOrder;
use readyset::internal::IndexType;

use crate::inner::Inner;
use crate::read::ReadHandle;
use crate::write::WriteHandle;

mod error;
mod eviction;
mod inner;
mod read;
mod values;
mod write;

pub use error::{Error, Result};

/// Handles to the read and write halves of an `reader_map`.
pub mod handles {
    pub use crate::read::ReadHandle;
    pub use crate::write::WriteHandle;
}

/// Helper types that give access to values inside the read half of an `reader_map`.
pub mod refs {
    // Same here, ::{..} won't work.
    // Expose `ReadGuard` since it has useful methods the user will likely care about.
    #[doc(inline)]
    pub use left_right::ReadGuard;

    pub use super::values::{Values, ValuesIter};
    pub use crate::read::{MapReadRef, Miss, ReadGuardIter};
}

/// Options for how to initialize the map.
///
/// In particular, the options dictate the hashing function, meta type, and initial capacity of the
/// map.
#[must_use]
pub struct Options<M, T, S, I> {
    meta: M,
    timestamp: T,
    hasher: S,
    index_type: IndexType,
    capacity: Option<usize>,
    eviction_strategy: EvictionStrategy,
    insertion_order: Option<I>,
}

impl<M, T, S, I> fmt::Debug for Options<M, T, S, I>
where
    M: fmt::Debug,
    T: fmt::Debug,
    I: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("meta", &self.meta)
            .field("timestamp", &self.timestamp)
            .field("capacity", &self.capacity)
            .field("order", &self.insertion_order)
            .finish()
    }
}

impl Default for Options<(), (), RandomState, DefaultInsertionOrder> {
    fn default() -> Self {
        Options {
            meta: (),
            timestamp: (),
            hasher: RandomState::default(),
            index_type: IndexType::BTreeMap,
            capacity: None,
            eviction_strategy: Default::default(),
            insertion_order: None,
        }
    }
}

impl<M, T, S, I> Options<M, T, S, I> {
    /// Set the initial meta value for the map.
    pub fn with_meta<M2>(self, meta: M2) -> Options<M2, T, S, I> {
        Options {
            meta,
            timestamp: self.timestamp,
            index_type: self.index_type,
            hasher: self.hasher,
            capacity: self.capacity,
            eviction_strategy: self.eviction_strategy,
            insertion_order: self.insertion_order,
        }
    }

    /// Set the hasher used for the map.
    pub fn with_hasher<S2>(self, hash_builder: S2) -> Options<M, T, S2, I> {
        Options {
            meta: self.meta,
            timestamp: self.timestamp,
            index_type: self.index_type,
            hasher: hash_builder,
            capacity: self.capacity,
            eviction_strategy: self.eviction_strategy,
            insertion_order: self.insertion_order,
        }
    }

    /// Set the initial capacity for the map.
    pub fn with_capacity(self, capacity: usize) -> Options<M, T, S, I> {
        Options {
            meta: self.meta,
            timestamp: self.timestamp,
            index_type: self.index_type,
            hasher: self.hasher,
            capacity: Some(capacity),
            eviction_strategy: self.eviction_strategy,
            insertion_order: self.insertion_order,
        }
    }

    /// Sets the initial timestamp of the map.
    pub fn with_timestamp<T2>(self, timestamp: T2) -> Options<M, T2, S, I> {
        Options {
            meta: self.meta,
            timestamp,
            index_type: self.index_type,
            hasher: self.hasher,
            capacity: self.capacity,
            eviction_strategy: self.eviction_strategy,
            insertion_order: self.insertion_order,
        }
    }

    /// Sets the desired [`InsertionOrder`] for the map
    pub fn with_insertion_order<I2>(self, insertion_order: Option<I2>) -> Options<M, T, S, I2> {
        Options {
            meta: self.meta,
            timestamp: self.timestamp,
            index_type: self.index_type,
            hasher: self.hasher,
            capacity: self.capacity,
            eviction_strategy: self.eviction_strategy,
            insertion_order,
        }
    }

    /// Sets the index type of the map.
    pub fn with_index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Sets the eviction strategy for the map.
    pub fn with_eviction_strategy(mut self, eviction_strategy: EvictionStrategy) -> Self {
        self.eviction_strategy = eviction_strategy;
        self
    }

    /// Create the map, and construct the read and write handles used to access it.
    #[allow(clippy::type_complexity)]
    pub fn construct<K, V>(self) -> (WriteHandle<K, V, I, M, T, S>, ReadHandle<K, V, I, M, T, S>)
    where
        K: Ord + Clone + Hash,
        S: BuildHasher + Clone,
        V: Ord + Clone,
        M: 'static + Clone,
        T: Clone,
        I: partial_map::InsertionOrder<V> + Clone,
    {
        let inner = Inner::with_index_type_and_hasher(
            self.index_type,
            self.meta,
            self.timestamp,
            self.hasher,
            self.eviction_strategy,
            self.insertion_order,
        );

        let (mut w, r) = left_right::new_from_empty(inner);
        w.append(write::Operation::MarkReady);

        (WriteHandle::new(w), ReadHandle::new(r))
    }
}

/// The default order of rows in the reader is the default order as defined by
/// [`slice::binary_search`]
#[derive(Clone, Debug)]
pub struct DefaultInsertionOrder {}

impl<V> InsertionOrder<V> for DefaultInsertionOrder
where
    V: Ord,
{
    fn get_insertion_order(&self, values: &[V], elem: &V) -> std::result::Result<usize, usize> {
        values.binary_search(elem)
    }
}

/// Create an empty eventually consistent map.
///
/// Use the [`Options`](./struct.Options.html) builder for more control over initialization.
#[allow(clippy::type_complexity)]
pub fn new<K, V>() -> (
    WriteHandle<K, V, DefaultInsertionOrder, (), (), RandomState>,
    ReadHandle<K, V, DefaultInsertionOrder, (), (), RandomState>,
)
where
    K: Ord + Clone + Hash,
    V: Ord + Clone,
{
    Options::default().construct()
}
