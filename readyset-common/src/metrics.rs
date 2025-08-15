//! Module for automatically wrapping and reporting the size of various data structures.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use metrics::{Gauge, gauge};

use crate::Len;

pub struct LenMetric<T> {
    inner: T,
    gauge: Gauge,
}

impl<T> LenMetric<T> {
    fn make_meta<K, V>(meta: &[(K, V)]) -> HashMap<String, String>
    where
        K: Borrow<str>,
        V: Borrow<str>,
    {
        meta.iter()
            .map(|(k, v)| (k.borrow().to_owned(), v.borrow().to_owned()))
            .collect()
    }

    fn make_len_name<S>(name: S) -> String
    where
        S: Borrow<str>,
    {
        format!("{}_len", name.borrow())
    }
}

impl<T> LenMetric<T>
where
    T: Default,
{
    pub fn new<S>(name: S) -> Self
    where
        S: Borrow<str>,
    {
        Self {
            inner: Default::default(),
            gauge: gauge!(Self::make_len_name(name)),
        }
    }

    pub fn new_meta<S, K, V>(name: S, meta: &[(K, V)]) -> Self
    where
        S: Borrow<str>,
        K: Borrow<str>,
        V: Borrow<str>,
    {
        Self {
            inner: Default::default(),
            gauge: gauge!(Self::make_len_name(name), &Self::make_meta(meta)),
        }
    }
}

impl<T> LenMetric<T> {
    pub fn new_from<F, S, K, V>(x: F, name: S, meta: &[(K, V)]) -> Self
    where
        T: From<F>,
        S: Borrow<str>,
        K: Borrow<str>,
        V: Borrow<str>,
    {
        Self {
            inner: x.into(),
            gauge: gauge!(Self::make_len_name(name), &Self::make_meta(meta)),
        }
    }
}

impl<T> LenMetric<T>
where
    T: Len,
{
    fn set(&self) {
        self.gauge.set(self.inner.len() as f64);
    }
}

impl<T> Deref for LenMetric<T>
where
    T: Len,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.set();
        &self.inner
    }
}

impl<T> DerefMut for LenMetric<T>
where
    T: Len,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.set();
        &mut self.inner
    }
}
