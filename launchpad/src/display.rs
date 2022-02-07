//! Utilities for human-readable display of values

use std::fmt::{self, Display, Write};

/// Extension trait allowing converting iterators over types that implement [`Display`] into English
/// lists with commas and conjunctions.
pub trait EnglishList: Sized {
    /// Write this value as an English list with commas using the given conjunction
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::display::EnglishList;
    ///
    /// let mut one_element = String::new();
    /// vec!["one"]
    ///     .write_list_with_conjunction("and then", &mut one_element)
    ///     .unwrap();
    /// assert_eq!(one_element, "one");
    ///
    /// // Commas aren't used for only two elements
    /// let mut two_elements = String::new();
    /// vec!["one", "two"]
    ///     .write_list_with_conjunction("and then", &mut two_elements)
    ///     .unwrap();
    /// assert_eq!(two_elements, "one and then two");
    ///
    /// let mut three_elements = String::new();
    /// vec!["one", "two", "three"]
    ///     .write_list_with_conjunction("and then", &mut three_elements)
    ///     .unwrap();
    /// assert_eq!(three_elements, "one, two, and then three");
    /// ```
    fn write_list_with_conjunction<W>(self, conjunction: &str, f: &mut W) -> fmt::Result
    where
        W: Write;

    /// Write this value as an English list with commas and "and" as the conjunction
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::display::EnglishList;
    ///
    /// let mut one_element = String::new();
    /// vec!["one"].write_and_list(&mut one_element).unwrap();
    /// assert_eq!(one_element, "one");
    ///
    /// // Commas aren't used for only two elements
    /// let mut two_elements = String::new();
    /// vec!["one", "two"]
    ///     .write_and_list(&mut two_elements)
    ///     .unwrap();
    /// assert_eq!(two_elements, "one and two");
    ///
    /// let mut three_elements = String::new();
    /// vec!["one", "two", "three"]
    ///     .write_and_list(&mut three_elements)
    ///     .unwrap();
    /// assert_eq!(three_elements, "one, two, and three");
    /// ```
    fn write_and_list<W>(self, f: &mut W) -> fmt::Result
    where
        W: Write,
    {
        self.write_list_with_conjunction("and", f)
    }

    /// Write this value as an English list with commas and "or" as the conjunction
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::display::EnglishList;
    ///
    /// let mut one_element = String::new();
    /// vec!["one"].write_or_list(&mut one_element).unwrap();
    /// assert_eq!(one_element, "one");
    ///
    /// // Commas aren't used for only two elements
    /// let mut two_elements = String::new();
    /// vec!["one", "two"].write_or_list(&mut two_elements).unwrap();
    /// assert_eq!(two_elements, "one or two");
    ///
    /// let mut three_elements = String::new();
    /// vec!["one", "two", "three"]
    ///     .write_or_list(&mut three_elements)
    ///     .unwrap();
    /// assert_eq!(three_elements, "one, two, or three");
    /// ```
    fn write_or_list<W>(self, f: &mut W) -> fmt::Result
    where
        W: Write,
    {
        self.write_list_with_conjunction("or", f)
    }

    /// Convert this value into an English list with commas and "and" as the conjunction
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::display::EnglishList;
    ///
    /// let one_element = vec!["one"].into_and_list();
    /// assert_eq!(one_element, "one");
    ///
    /// // Commas aren't used for only two elements
    /// let two_elements = vec!["one", "two"].into_and_list();
    /// assert_eq!(two_elements, "one and two");
    ///
    /// let three_elements = vec!["one", "two", "three"].into_and_list();
    /// assert_eq!(three_elements, "one, two, and three");
    /// ```
    fn into_and_list(self) -> String {
        let mut res = String::new();
        self.write_and_list(&mut res).unwrap();
        res
    }

    /// Convert this value into an English list with commas and "or" as the conjunction
    ///
    /// # Examples
    ///
    /// ```rust
    /// use launchpad::display::EnglishList;
    ///
    /// let one_element = vec!["one"].into_or_list();
    /// assert_eq!(one_element, "one");
    ///
    /// // Commas aren't used for only two elements
    /// let two_elements = vec!["one", "two"].into_or_list();
    /// assert_eq!(two_elements, "one or two");
    ///
    /// let three_elements = vec!["one", "two", "three"].into_or_list();
    /// assert_eq!(three_elements, "one, two, or three");
    /// ```
    fn into_or_list(self) -> String {
        let mut res = String::new();
        self.write_or_list(&mut res).unwrap();
        res
    }
}

impl<T> EnglishList for T
where
    T: IntoIterator,
    T::Item: Display,
    T::IntoIter: ExactSizeIterator,
{
    fn write_list_with_conjunction<W>(self, conjunction: &str, f: &mut W) -> fmt::Result
    where
        W: Write,
    {
        let iter = self.into_iter();
        let len = iter.len();
        for (i, val) in iter.enumerate() {
            if i == len - 1 && len != 1 {
                if len != 2 {
                    write!(f, ",")?;
                }
                write!(f, " {} ", conjunction)?;
            } else if i != 0 {
                write!(f, ", ")?;
            }

            write!(f, "{}", val)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;

    #[proptest]
    fn all_elements_in_string(elems: Vec<String>, conjunction: String) {
        let mut res = String::new();
        elems
            .clone()
            .write_list_with_conjunction(&conjunction, &mut res)
            .unwrap();

        for elem in elems {
            assert!(res.contains(&elem))
        }
    }
}
