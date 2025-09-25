use log::{Level, Record};

#[derive(Clone)]
pub struct Logger {
    pub enabled: bool,
    pub target: String,
}

impl Logger {
    pub fn new(enabled: bool, target: impl Into<String>) -> Self {
        Self {
            enabled,
            target: target.into(),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
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

    #[inline]
    pub fn trace_args(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Trace, args)
    }

    #[inline]
    pub fn debug_args(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Debug, args)
    }

    #[inline]
    pub fn info_args(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Info, args)
    }

    #[inline]
    pub fn warn_args(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Warn, args)
    }

    #[inline]
    pub fn error_args(&self, args: std::fmt::Arguments) {
        self.log_args(Level::Error, args)
    }
}

#[macro_export]
macro_rules! tracef {
    ($logger:expr, $($arg:tt)+) => {
        $logger.trace_args(format_args!($($arg)+))
    };
}

#[macro_export]
macro_rules! debugf {
    ($logger:expr, $($arg:tt)+) => {
        $logger.debug_args(format_args!($($arg)+))
    };
}

#[macro_export]
macro_rules! infof {
    ($logger:expr, $($arg:tt)+) => {
        $logger.info_args(format_args!($($arg)+))
    };
}

#[macro_export]
macro_rules! warnf {
    ($logger:expr, $($arg:tt)+) => {
        $logger.warn_args(format_args!($($arg)+))
    };
}

#[macro_export]
macro_rules! errorf {
    ($logger:expr, $($arg:tt)+) => {
        $logger.error_args(format_args!($($arg)+))
    };
}

#[cfg(test)]
mod tests {
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

        debugf!(logger, "debug message {}", 1);
        infof!(logger, "info message");
        warnf!(logger, "warning!");
        errorf!(logger, "error!");

        let logs = buf.lock().unwrap().clone();

        assert!(logs.iter().any(|l| l.contains("debug message 1")));
        assert!(logs.iter().any(|l| l.contains("info message")));
        assert!(logs.iter().any(|l| l.contains("warning!")));
        assert!(logs.iter().any(|l| l.contains("error!")));
    }
}
