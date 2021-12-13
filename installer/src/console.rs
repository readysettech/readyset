use std::fmt::{Debug, Display};
use std::str::FromStr;

use console::{style, Emoji, StyledObject};
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Password, Select};
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref DIALOG_THEME: ColorfulTheme = ColorfulTheme::default();
    pub(crate) static ref SPINNER_STYLE: ProgressStyle = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("{prefix:.bold.dim} {spinner} {wide_msg}");
    pub(crate) static ref GREEN_CHECK: StyledObject<::console::Emoji<'static, 'static>> =
        style(Emoji("✔ ", "")).green();
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

pub(crate) fn password() -> Password<'static> {
    Password::with_theme(&*DIALOG_THEME)
}

pub(crate) fn select() -> Select<'static> {
    Select::with_theme(&*DIALOG_THEME)
}

macro_rules! success {
    ($($format_args:tt)*) => {
        println!(
            "\n{}{}\n",
            *crate::console::GREEN_CHECK,
            style(format_args!($($format_args)*)).bold()
        )
    };
}

macro_rules! warning {
	($($format_args:tt)*) => {
	    println!(
            "{}{}",
            style(::console::Emoji("⚠ ", "")).yellow(),
            format_args!($($format_args)*)
        )
	};
}

pub(crate) fn spinner() -> ProgressBar {
    let spinner = ProgressBar::new_spinner().with_style(SPINNER_STYLE.clone());
    spinner.enable_steady_tick(50);
    spinner
}
