use crate::{cfg::TurboLogLevel, utils::likely};
use log::{Level, Record};

const TARGET: &'static str = "TurboFox";

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

pub(crate) enum LogCtx {
    Cfg,
}

impl LogCtx {
    fn to_ok(&self) -> String {
        match self {
            Self::Cfg => "CFGO".into(),
        }
    }

    fn to_err(&self) -> String {
        match self {
            Self::Cfg => "CFGE".into(),
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

impl Logger {
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

    #[inline]
    pub(crate) const fn enable(&mut self, enable: bool) {
        self.enabled = enable;
    }

    #[inline]
    pub(crate) const fn set_level(&mut self, level: TurboLogLevel) {
        self.level = level;
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

#[cfg(test)]
pub(crate) fn test_logger(target: &'static str) -> Logger {
    let _ = env_logger::builder().is_test(true).try_init();
    Logger {
        target,
        enabled: true,
        level: TurboLogLevel::TRACE,
    }
}
