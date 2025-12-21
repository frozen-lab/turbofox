use crate::{cfg::TurboLogLevel, utils::likely};
use log::{Level, Record};

const TARGET: &'static str = "TurboFox";

pub(crate) enum LogCtx {
    Dir,
}

impl LogCtx {
    fn to_ok(&self) -> String {
        match self {
            Self::Dir => "DIRO".into(),
        }
    }

    fn to_err(&self) -> String {
        match self {
            Self::Dir => "DIRE".into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Logger {
    enabled: bool,
    target: &'static str,
    level: TurboLogLevel,
}

impl Logger {
    #[inline]
    pub(crate) const fn new(enabled: bool, level: TurboLogLevel) -> Self {
        Self {
            enabled,
            target: TARGET,
            level,
        }
    }

    #[inline]
    pub(crate) fn trace(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        // NOTE: As logging is turned off by default, and/or in prod env's trace
        // logs should/will be turned off, we are most likely to do notihing here,
        // so *likely* would help us avoid branch misses!
        if likely(self.level > TurboLogLevel::TRACE) {
            return;
        }

        self.log(Level::Trace, format_args!("({}) {args}", ctx.to_ok()));
    }

    #[inline]
    pub(crate) fn info(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Info, format_args!("({}) {args}", ctx.to_ok()));
    }

    #[inline]
    pub(crate) fn warn(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Warn, format_args!("({}) {args}", ctx.to_err()));
    }

    #[inline]
    pub(crate) fn error(&self, ctx: LogCtx, args: impl std::fmt::Display) {
        self.log(Level::Error, format_args!("({}) {args}", ctx.to_err()));
    }

    #[inline(always)]
    fn log(&self, level: Level, args: std::fmt::Arguments) {
        // deny logging based on `[Level]`
        if !self.enabled || (from_level(level) > self.level) {
            return;
        }

        let record = Record::builder().args(args).level(level).target(&self.target).build();
        log::logger().log(&record);
    }
}

#[inline]
const fn from_level(level: Level) -> TurboLogLevel {
    match level {
        Level::Error => TurboLogLevel::ERROR,
        Level::Warn => TurboLogLevel::WARN,
        Level::Info => TurboLogLevel::INFO,
        Level::Trace => TurboLogLevel::TRACE,
        _ => unreachable!(),
    }
}

#[cfg(test)]
pub(crate) fn test_logger(target: &'static str) -> Logger {
    let _ = env_logger::builder().is_test(true).try_init();
    Logger {
        target,
        enabled: true,
        level: TurboLogLevel::TRACE,
    }
}
