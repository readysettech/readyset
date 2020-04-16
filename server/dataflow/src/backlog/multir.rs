use ahash::RandomState;
use common::DataType;
use evmap;

#[derive(Clone)]
pub(super) enum Handle {
    Single(evmap::ReadHandle<DataType, Vec<DataType>, i64, RandomState>),
    Double(evmap::ReadHandle<(DataType, DataType), Vec<DataType>, i64, RandomState>),
    Many(evmap::ReadHandle<Vec<DataType>, Vec<DataType>, i64, RandomState>),
}

impl Handle {
    pub(super) fn len(&self) -> usize {
        match *self {
            Handle::Single(ref h) => h.len(),
            Handle::Double(ref h) => h.len(),
            Handle::Many(ref h) => h.len(),
        }
    }

    pub(super) fn meta_get_and<F, T>(&self, key: &[DataType], then: F) -> Option<(Option<T>, i64)>
    where
        F: FnOnce(&evmap::Values<Vec<DataType>, RandomState>) -> T,
    {
        match *self {
            Handle::Single(ref h) => {
                assert_eq!(key.len(), 1);
                let map = h.read();
                let v = map.get(&key[0]).map(then);
                map.meta().cloned().map(move |m| (v, m))
            }
            Handle::Double(ref h) => {
                assert_eq!(key.len(), 2);
                // we want to transmute &[T; 2] to &(T, T), but that's not actually safe
                // we're not guaranteed that they have the same memory layout
                // we *could* just clone DataType, but that would mean dealing with string refcounts
                // so instead, we play a trick where we memcopy onto the stack and then forget!
                //
                // h/t https://gist.github.com/mitsuhiko/f6478a0dd1ef174b33c63d905babc89a
                use std::mem;
                use std::ptr;
                unsafe {
                    let mut stack_key: (mem::MaybeUninit<DataType>, mem::MaybeUninit<DataType>) =
                        (mem::MaybeUninit::uninit(), mem::MaybeUninit::uninit());
                    ptr::copy_nonoverlapping(
                        &key[0] as *const DataType,
                        stack_key.0.as_mut_ptr(),
                        1,
                    );
                    ptr::copy_nonoverlapping(
                        &key[1] as *const DataType,
                        stack_key.1.as_mut_ptr(),
                        1,
                    );
                    let stack_key = mem::transmute::<_, &(DataType, DataType)>(&stack_key);
                    let map = h.read();
                    let v = map.get(&stack_key).map(then);
                    map.meta().cloned().map(move |m| (v, m))
                }
            }
            Handle::Many(ref h) => {
                let map = h.read();
                let v = map.get(key).map(then);
                map.meta().cloned().map(move |m| (v, m))
            }
        }
    }
}
