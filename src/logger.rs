use log::{Level, Record};

#[cfg(test)]
pub(crate) fn init_test_logger(target: &'static str) -> Logger {
    let _ = env_logger::builder().is_test(true).try_init();
    Logger::new(true, target)
}

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

        let record = Record::builder().args(args).level(level).target(&self.target).build();

        log::logger().log(&record);
    }

    #[inline(always)]
    pub fn trace(&self, args: impl std::fmt::Display) {
        self.log_args(Level::Trace, format_args!("{args}"));
    }

    #[inline(always)]
    pub fn debug(&self, args: impl std::fmt::Display) {
        self.log_args(Level::Debug, format_args!("{args}"));
    }

    #[inline(always)]
    pub fn info(&self, args: impl std::fmt::Display) {
        self.log_args(Level::Info, format_args!("{args}"));
    }

    #[inline(always)]
    pub fn warn(&self, args: impl std::fmt::Display) {
        self.log_args(Level::Warn, format_args!("{args}"));
    }

    #[inline(always)]
    pub fn error(&self, args: impl std::fmt::Display) {
        self.log_args(Level::Error, format_args!("{args}"));
    }
}
