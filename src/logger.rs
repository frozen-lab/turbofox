use log::{Level, Record};

///
/// Logging for Test modules
///

#[cfg(test)]
static INIT: std::sync::Once = std::sync::Once::new();

#[cfg(test)]
pub fn init_test_logger() {
    INIT.call_once(|| {
        let _ = env_logger::builder().is_test(true).try_init();
    });
}

///
/// Logger
///

#[derive(Debug, Clone, Copy)]
pub(crate) struct Logger {
    pub enabled: bool,
    pub target: &'static str,
}

impl Logger {
    #[inline(always)]
    pub fn new(enabled: bool, target: &'static str) -> Self {
        Self { enabled, target }
    }

    #[inline(always)]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn log_args(&self, level: Level, args: std::fmt::Arguments) {
        if !self.enabled {
            return;
        }

        let record = Record::builder()
            .args(args)
            .level(level)
            .target(&self.target)
            .build();

        log::logger().log(&record);
    }

    #[inline(always)]
    pub fn trace(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Trace, args);
    }

    #[inline(always)]
    pub fn debug(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Debug, args);
    }

    #[allow(unused)]
    #[inline(always)]
    pub fn info(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Info, args);
    }

    #[allow(unused)]
    #[inline(always)]
    pub fn warn(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Warn, args);
    }

    #[inline(always)]
    pub fn error(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Error, args);
    }
}

///
/// Macros for Logger
///

#[macro_export]
macro_rules! log_trace {
    ($logger:expr, $($arg:tt)*) => {{
        if $logger.is_enabled() {
            $logger.trace(format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($arg:tt)*) => {{
        if $logger.is_enabled() {
            $logger.debug(format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($arg:tt)*) => {{
        if $logger.is_enabled() {
            $logger.info(format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($arg:tt)*) => {{
        if $logger.is_enabled() {
            $logger.warn(format_args!($($arg)*));
        }
    }};
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($arg:tt)*) => {{
        if $logger.is_enabled() {
            $logger.error(format_args!($($arg)*));
        }
    }};
}

///
/// Debug Logger
///

#[cfg(test)]
pub(crate) use debug_logger::DebugLogger;

#[cfg(test)]
mod debug_logger {
    use log::{Level, Record};
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn ensure_env_logger() {
        INIT.call_once(|| {
            let _ = env_logger::builder().is_test(true).try_init();
        });
    }

    pub(crate) struct DebugLogger;

    impl DebugLogger {
        #[inline(always)]
        pub fn log(level: Level, args: std::fmt::Arguments) {
            ensure_env_logger();

            let record = Record::builder()
                .args(args)
                .level(level)
                .target("turbocache::debug")
                .build();

            log::logger().log(&record);
        }
    }
}

///
/// Macros for Debug Logger
///

#[macro_export]
macro_rules! debug_trace {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        $crate::logger::DebugLogger::log(log::Level::Trace, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! debug_debug {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        $crate::logger::DebugLogger::log(log::Level::Debug, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! debug_info {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        $crate::logger::DebugLogger::log(log::Level::Info, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! debug_warn {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        $crate::logger::DebugLogger::log(log::Level::Warn, format_args!($($arg)*));
    }};
}

#[macro_export]
macro_rules! debug_error {
    ($($arg:tt)*) => {{
        #[cfg(test)]
        $crate::logger::DebugLogger::log(log::Level::Error, format_args!($($arg)*));
    }};
}
