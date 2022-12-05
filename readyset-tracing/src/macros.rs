#[macro_export]
macro_rules! trace {
    (target: $target:expr, $($tts:tt)+) => {
        ::tracing::trace!(target: $target, context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    };
    ($($tts:tt)*) => {
        ::tracing::trace!(context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    }
}

#[macro_export]
macro_rules! debug {
    (target: $target:expr, $($tts:tt)+) => {
        ::tracing::debug!(target: $target, context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    };
    ($($tts:tt)*) => {
        ::tracing::debug!(context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    }
}

#[macro_export]
macro_rules! info {
    (target: $target:expr, $($tts:tt)+) => {
        ::tracing::info!(target: $target, context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    };
    ($($tts:tt)*) => {
        ::tracing::info!(context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    }
}

#[macro_export]
macro_rules! warn {
    (target: $target:expr, $($tts:tt)+) => {
        ::tracing::warn!(target: $target, context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    };
    ($($tts:tt)*) => {
        ::tracing::warn!(context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    }
}

#[macro_export]
macro_rules! error {
    (target: $target:expr, $($tts:tt)+) => {
        ::tracing::error!(target: $target, context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    };
    ($($tts:tt)*) => {
        ::tracing::error!(context=%*readyset_tracing::tracing_wrapper::LOG_CONTEXT, $($tts)*)
    }
}
