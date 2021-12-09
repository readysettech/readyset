use std::fmt::{Debug, Display};
use std::str::FromStr;

use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};
use indicatif::ProgressStyle;
use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref DIALOG_THEME: ColorfulTheme = ColorfulTheme::default();
    pub(crate) static ref SPINNER_STYLE: ProgressStyle = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("{prefix:.bold.dim} {spinner} {wide_msg}");
}

pub(crate) fn confirm() -> Confirm<'static> {
    let mut confirm = Confirm::with_theme(&*DIALOG_THEME);
    confirm.wait_for_newline(true);
    confirm
}

pub(crate) fn input<T>() -> Input<'static, T>
where
    T: Clone + FromStr + Display,
    T::Err: Display + Debug,
{
    Input::with_theme(&*DIALOG_THEME)
}

pub(crate) fn select() -> Select<'static> {
    Select::with_theme(&*DIALOG_THEME)
}

macro_rules! success {
    ($($format_args:tt)*) => {
        println!(
            "\n{}{}\n",
            style(::console::Emoji("✔ ", "")).green(),
            style(format_args!($($format_args)*)).bold()
        )
    };
}
