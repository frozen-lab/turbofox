use log::{Level, Record};

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

#[cfg(test)]
mod logger_tests {
    use super::*;

    mod logger {
        use super::*;
        use log::{Level, Metadata, Record};
        use once_cell::sync::OnceCell;
        use std::sync::{Arc, Mutex};

        // a dummy looger that writes into shared buffer
        struct DummyLogger {
            buf: Arc<Mutex<Vec<String>>>,
            level: Level,
        }

        impl log::Log for DummyLogger {
            fn enabled(&self, metadata: &Metadata) -> bool {
                metadata.level() <= self.level
            }

            fn log(&self, record: &Record) {
                if self.enabled(record.metadata()) {
                    let msg = format!(
                        "[{}][{}] {}",
                        record.level(),
                        record.target(),
                        record.args()
                    );

                    self.buf.lock().unwrap().push(msg);
                }
            }

            fn flush(&self) {}
        }

        static INIT: OnceCell<Arc<Mutex<Vec<String>>>> = OnceCell::new();

        fn init_test_logger(level: Level) -> Arc<Mutex<Vec<String>>> {
            INIT.get_or_init(|| {
                let buf = Arc::new(Mutex::new(Vec::new()));
                let logger = DummyLogger {
                    buf: buf.clone(),
                    level,
                };
                let _ = log::set_boxed_logger(Box::new(logger));

                log::set_max_level(level.to_level_filter());
                buf
            })
            .clone()
        }

        #[test]
        fn test_logging() {
            let buf = init_test_logger(Level::Trace);
            let logger = Logger::new(true, "unit_test");

            log_debug!(logger, "debug message {}", 1);
            log_error!(logger, "info message");
            log_warn!(logger, "warning!");
            log_error!(logger, "error!");

            let logs = buf.lock().unwrap();

            assert!(logs.iter().any(|l| l.contains("debug message 1")));
            assert!(logs.iter().any(|l| l.contains("info message")));
            assert!(logs.iter().any(|l| l.contains("warning!")));
            assert!(logs.iter().any(|l| l.contains("error!")));
        }
    }
}
