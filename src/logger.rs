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

#[derive(Debug, Clone)]
pub(crate) struct Logger {
    pub enabled: bool,
    pub target: String,
}

impl Logger {
    #[inline(always)]
    pub fn new(enabled: bool, target: impl Into<String>) -> Self {
        Self {
            enabled,
            target: target.into(),
        }
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

    #[inline(always)]
    pub fn info(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Info, args);
    }

    #[inline(always)]
    pub fn warn(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Warn, args);
    }

    #[inline(always)]
    pub fn error(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Error, args);
    }
}

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

#[deprecated(since = "0.1.2", note = "Use `Logger` instead")]
#[allow(unused)]
pub(crate) struct DebugLogger;

#[allow(unused)]
impl DebugLogger {
    #[inline(always)]
    pub fn log(level: &str, args: std::fmt::Arguments) {
        #[cfg(debug_assertions)]
        {
            if level == "ERROR" || level == "WARN" {
                eprintln!("[{}] {}", level, args);
            } else {
                println!("[{}] {}", level, args);
            }
        }
    }
}

#[deprecated(since = "0.1.2", note = "Use `Logger` instead")]
#[macro_export]
macro_rules! debug_trace {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        $crate::logger::DebugLogger::log("TRACE", format_args!($($arg)*));
    };
}

#[deprecated(since = "0.1.2", note = "Use `Logger` instead")]
#[macro_export]
macro_rules! debug_debug {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        $crate::logger::DebugLogger::log("DEBUG", format_args!($($arg)*));
    };
}

#[deprecated(since = "0.1.2", note = "Use `Logger` instead")]
#[macro_export]
macro_rules! debug_info {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        $crate::logger::DebugLogger::log("INFO", format_args!($($arg)*));
    };
}

#[deprecated(since = "0.1.2", note = "Use `Logger` instead")]
#[macro_export]
macro_rules! debug_warn {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        $crate::logger::DebugLogger::log("WARN", format_args!($($arg)*));
    };
}

#[deprecated(since = "0.1.2", note = "Use `Logger` instead")]
#[macro_export]
macro_rules! debug_error {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        $crate::logger::DebugLogger::log("ERROR", format_args!($($arg)*));
    };
}
