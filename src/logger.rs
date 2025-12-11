use log::{Level, Record};

pub(crate) enum LogCtx {
    Cfg,
    InvDb,
    Cache,
}

impl LogCtx {
    fn to_str(&self) -> String {
        match self {
            Self::Cfg => "CFGC".into(),
            Self::InvDb => "INDB".into(),
            Self::Cache => "CCHE".into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Logger {
    enabled: bool,
    target: &'static str,
    level: TurboLogLevel,
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

const TARGET: &'static str = "TurboFox";

impl Logger {
    #[inline]
    pub(crate) fn trace(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Trace, format_args!("({}){args}", ctx.to_str()));
    }

    #[inline]
    pub(crate) fn info(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Info, format_args!("({}){args}", ctx.to_str()));
    }

    #[inline]
    pub(crate) fn warn(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Warn, format_args!("({}){args}", ctx.to_str()));
    }

    #[inline]
    pub(crate) fn error(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Error, format_args!("({}){args}", ctx.to_str()));
    }

    #[inline]
    pub(crate) const fn enable(&mut self) {
        self.enabled = true;
    }

    #[inline]
    pub(crate) const fn disable(&mut self) {
        self.enabled = false;
    }

    #[inline]
    pub(crate) const fn set_level(&mut self, level: TurboLogLevel) {
        self.level = level;
    }

    #[inline(always)]
    fn log(&self, level: Level, args: std::fmt::Arguments) {
        // deny logging based on `[Level]`
        if !self.enabled || (TurboLogLevel::from_level(level) > self.level) {
            return;
        }

        let record = Record::builder().args(args).level(level).target(&self.target).build();
        log::logger().log(&record);
    }
}

/// Allowed log levels for `[TurboFox]` db
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub enum TurboLogLevel {
    /// Allows only errors to be logged
    ERROR = 0x00,

    /// Allows warnings and errors to be logged
    WARN = 0x01,

    /// Allows info, warning and errors to be logged
    INFO = 0x02,

    /// Allows trace, info, warning and errors to be logged
    ///
    /// ## NOTE
    ///
    /// Only use this in debubg mode, as this log level will log db every operation,
    /// and would end up cluttering your logs.
    TRACE = 0x03,
}

impl TurboLogLevel {
    #[inline]
    const fn from_level(level: Level) -> Self {
        match level {
            Level::Error => TurboLogLevel::ERROR,
            Level::Warn => TurboLogLevel::WARN,
            Level::Info => TurboLogLevel::INFO,
            Level::Trace => TurboLogLevel::TRACE,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_logger(target: &'static str) -> Logger {
    let _ = env_logger::builder().is_test(true).try_init();

    Logger {
        enabled: true,
        target,
        level: TurboLogLevel::TRACE,
    }
}
