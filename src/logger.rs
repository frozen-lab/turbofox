use log::{Level, Record};

const TARGET: &'static str = "TurboFox";

#[derive(Debug, Clone)]
pub(crate) struct Logger {
    pub(crate) enabled: bool,
    pub(crate) target: &'static str,
    pub(crate) level: TurboLogLevel,
}

impl Default for Logger {
    #[inline]
    fn default() -> Self {
        Self {
            enabled: false,
            target: TARGET,
            level: TurboLogLevel::ERROR,
        }
    }
}

impl Logger {
    pub(crate) const fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn trace(&self, args: impl std::fmt::Display) {
        self._log(Level::Trace, format_args!("{args}"));
    }

    pub(crate) fn debug(&self, args: impl std::fmt::Display) {
        self._log(Level::Debug, format_args!("{args}"));
    }

    pub(crate) fn info(&self, args: impl std::fmt::Display) {
        self._log(Level::Info, format_args!("{args}"));
    }

    pub(crate) fn warn(&self, args: impl std::fmt::Display) {
        self._log(Level::Warn, format_args!("{args}"));
    }

    pub(crate) fn error(&self, args: impl std::fmt::Display) {
        self._log(Level::Error, format_args!("{args}"));
    }

    #[cfg(test)]
    /// [Logger] instance for test modules
    pub(crate) fn test_logger(target: &'static str) -> Self {
        let _ = env_logger::builder().is_test(true).try_init();
        Logger {
            enabled: true,
            target,
            level: TurboLogLevel::TRACE,
        }
    }

    #[inline]
    fn _log(&self, level: Level, args: std::fmt::Arguments) {
        // deny logging
        if !self.enabled || (TurboLogLevel::from_level(level) > self.level) {
            return;
        }

        let record = Record::builder().args(args).level(level).target(&self.target).build();
        log::logger().log(&record);
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum TurboLogLevel {
    /// Used to log fatel errors via [Logger]
    ERROR = 0x00,

    /// Used to log warning messages via [Logger]
    WARN = 0x01,

    /// Used to log important information via [Logger]
    INFO = 0x02,

    /// Used to log debubg information via [Logger]
    ///
    /// **NOTE**: For internal use only
    #[doc(hidden)]
    #[cfg(test)]
    DEBUG = 0x03,

    /// Used to log traces and other debug info via [Logger]
    ///
    /// **NOTE**: For internal use only
    #[doc(hidden)]
    #[cfg(test)]
    TRACE = 0x04,
}

impl TurboLogLevel {
    #[inline]
    const fn from_level(level: Level) -> Self {
        match level {
            Level::Error => TurboLogLevel::ERROR,
            Level::Warn => TurboLogLevel::WARN,
            Level::Info => TurboLogLevel::INFO,
            #[cfg(test)]
            Level::Debug => TurboLogLevel::DEBUG,
            #[cfg(test)]
            Level::Trace => TurboLogLevel::TRACE,
            _ => unreachable!(),
        }
    }
}
