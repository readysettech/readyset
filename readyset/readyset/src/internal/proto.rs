use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(PartialEq)]
struct LocalBypass<T>(*mut T);

impl<T> LocalBypass<T> {
    pub fn make(t: Box<T>) -> Self {
        LocalBypass(Box::into_raw(t))
    }

    pub unsafe fn deref(&self) -> &T {
        &*self.0
    }

    pub unsafe fn take(self) -> Box<T> {
        Box::from_raw(self.0)
    }
}

unsafe impl<T> Send for LocalBypass<T> {}
impl<T> Serialize for LocalBypass<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (self.0 as usize).serialize(serializer)
    }
}
impl<'de, T> Deserialize<'de> for LocalBypass<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        usize::deserialize(deserializer).map(|p| LocalBypass(p as *mut T))
    }
}
impl<T> Clone for LocalBypass<T> {
    fn clone(&self) -> LocalBypass<T> {
        panic!("LocalBypass types cannot be cloned");
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
enum LocalOrNotInner<T> {
    Local(LocalBypass<T>),
    Not(T),
}

impl<T> LocalOrNotInner<T> {
    pub unsafe fn deref(&self) -> &T {
        match self {
            LocalOrNotInner::Local(ref l) => l.deref(),
            LocalOrNotInner::Not(ref t) => t,
        }
    }

    pub unsafe fn take(self) -> T {
        match self {
            LocalOrNotInner::Local(l) => *l.take(),
            LocalOrNotInner::Not(t) => t,
        }
    }
}

impl<T> Clone for LocalOrNotInner<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        LocalOrNotInner::Not(unsafe { self.deref() }.clone())
    }
}

#[doc(hidden)]
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct LocalOrNot<T>(LocalOrNotInner<T>);

impl<T> fmt::Debug for LocalOrNot<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = if let LocalOrNotInner::Local(..) = self.0 {
            "LocalOrNot::Local"
        } else {
            "LocalOrNot::Not"
        };
        fmt.debug_tuple(name)
            .field(unsafe { self.0.deref() })
            .finish()
    }
}

impl<T> LocalOrNot<T> {
    #[doc(hidden)]
    pub fn is_local(&self) -> bool {
        matches!(self.0, LocalOrNotInner::Local(..))
    }

    /// Creates a new LocalOrNot object based on the destinations
    /// locality. Unsafe when `dst_is_local` is true.
    ///
    /// # Safety
    ///
    /// When `dst_is_local` the internal object is boxed
    /// and may have ownership transfered, the LocalOrNot object
    /// still provides `take` and `deref` calls to an object that
    /// may not still be contained in the Box.
    pub unsafe fn new_for_dst(t: T, dst_is_local: bool) -> Self {
        if dst_is_local {
            LocalOrNot::for_local_transfer(t)
        } else {
            LocalOrNot::new(t)
        }
    }

    #[doc(hidden)]
    pub unsafe fn for_local_transfer(t: T) -> Self {
        LocalOrNot(LocalOrNotInner::Local(LocalBypass::make(Box::new(t))))
    }

    #[doc(hidden)]
    pub fn new(t: T) -> Self {
        LocalOrNot(LocalOrNotInner::Not(t))
    }

    #[doc(hidden)]
    #[allow(clippy::should_implement_trait)]
    pub unsafe fn deref(&self) -> &T {
        self.0.deref()
    }

    #[doc(hidden)]
    pub unsafe fn take(self) -> T {
        self.0.take()
    }
}

unsafe impl<T> Send for LocalOrNot<T> where T: Send {}
