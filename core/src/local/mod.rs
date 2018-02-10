use std::ops::Deref;
use std::rc::Rc;

mod single_state;
mod state;
mod keyed_state;

pub use data::DataType;
pub use self::state::State;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub struct Tag(pub u32);
impl Tag {
    pub fn id(&self) -> u32 {
        self.0
    }
}

pub struct Row<T>(pub(crate) Rc<T>);
unsafe impl<T> Send for Row<T> {}
impl<T> Deref for Row<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub enum LookupResult<'a> {
    Some(&'a [Row<Vec<DataType>>]),
    Missing,
}

#[derive(Clone)]
pub enum KeyType<'a> {
    Single(&'a DataType),
    Double((DataType, DataType)),
    Tri((DataType, DataType, DataType)),
    Quad((DataType, DataType, DataType, DataType)),
    Quin((DataType, DataType, DataType, DataType, DataType)),
    Sex((DataType, DataType, DataType, DataType, DataType, DataType)),
}

impl<'a> KeyType<'a> {
    pub fn from<I>(other: I) -> Self
    where
        I: IntoIterator<Item = &'a DataType>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let mut other = other.into_iter();
        let len = other.len();
        let mut more = move || other.next().unwrap();
        match len {
            0 => unreachable!(),
            1 => KeyType::Single(more()),
            2 => KeyType::Double((more().clone(), more().clone())),
            3 => KeyType::Tri((more().clone(), more().clone(), more().clone())),
            4 => KeyType::Quad((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            5 => KeyType::Quin((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            6 => KeyType::Sex((
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
                more().clone(),
            )),
            _ => unimplemented!(),
        }
    }
}
