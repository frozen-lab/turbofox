use log::{Level, Record};

#[derive(Clone)]
#[allow(unused)]
pub(crate) struct Logger {
    pub enabled: bool,
    pub target: String,
}

#[allow(unused)]
impl Logger {
    pub fn new(enabled: bool, target: impl Into<String>) -> Self {
        Self {
            enabled,
            target: target.into(),
        }
    }

    #[inline]
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
    pub fn trace(&self, msg: impl std::fmt::Display) {
        self.log_args(Level::Trace, format_args!("{}", msg))
    }

    #[inline(always)]
    pub fn debug(&self, msg: impl std::fmt::Display) {
        self.log_args(Level::Debug, format_args!("{}", msg))
    }

    #[inline(always)]
    pub fn info(&self, msg: impl std::fmt::Display) {
        self.log_args(Level::Info, format_args!("{}", msg))
    }

    #[inline(always)]
    pub fn warn(&self, msg: impl std::fmt::Display) {
        self.log_args(Level::Warn, format_args!("{}", msg))
    }

    #[inline(always)]
    pub fn error(&self, msg: impl std::fmt::Display) {
        self.log_args(Level::Error, format_args!("{}", msg))
    }
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

        logger.debug(format_args!("debug message {}", 1));
        logger.info("info message");
        logger.warn("warning!");
        logger.error("error!");

        let logs = buf.lock().unwrap().clone();

        assert!(logs.iter().any(|l| l.contains("debug message 1")));
        assert!(logs.iter().any(|l| l.contains("info message")));
        assert!(logs.iter().any(|l| l.contains("warning!")));
        assert!(logs.iter().any(|l| l.contains("error!")));
    }
}
