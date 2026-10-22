//! Shared, single-line console layout for application and SDK events.
use std::fmt;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    fmt::{FmtContext, FormatEvent, FormatFields, format::Writer},
    registry::LookupSpan,
};

pub fn init() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,a_bot=info".into()),
        )
        .with_ansi(false)
        .event_format(Layout)
        .with_writer(std::io::stderr)
        .init();
}
pub fn info(component: &str, message: impl fmt::Display) {
    tracing::info!(target: "a_bot", component, "{message}");
}
pub fn warn(component: &str, message: impl fmt::Display) {
    tracing::warn!(target: "a_bot", component, "{message}");
}
pub fn error(component: &str, message: impl fmt::Display) {
    tracing::error!(target: "a_bot", component, "{message}");
}
pub fn debug(component: &str, message: impl fmt::Display) {
    tracing::debug!(target: "a_bot", component, "{message}");
}

struct Layout;
#[derive(Default)]
struct Fields {
    component: String,
    message: String,
    extra: Vec<String>,
}
impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "component" => self.component = value.into(),
            "message" => self.message = value.into(),
            name => self.extra.push(format!("{name}={value}")),
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "component" => self.component = format!("{value:?}"),
            name => self.extra.push(format!("{name}={value:?}")),
        }
    }
}
fn line(time: &str, level: &str, component: &str, message: &str) -> String {
    let component: String = component.chars().take(12).collect();
    format!(
        "{time} {level:<5} {component:<12} {}\n",
        message.replace(['\r', '\n'], " ")
    )
}
impl<S, N> FormatEvent<S, N> for Layout
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.component.is_empty() {
            fields.component = if event.metadata().target().starts_with("a_bot") {
                "APP"
            } else {
                "SDK"
            }
            .into();
        }
        if !fields.extra.is_empty() {
            fields.message.push(' ');
            fields.message.push_str(&fields.extra.join(" "));
        }
        write!(
            writer,
            "{}",
            line(
                &chrono::Utc::now().format("%H:%M:%S%.3f").to_string(),
                event.metadata().level().as_str(),
                &fields.component,
                &fields.message
            )
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aligned_columns_and_single_line() {
        let info = line("12:34:56.789", "INFO", "WRITE", "rows=     1");
        let warn = line("12:34:56.789", "WARN", "SDK", "broken\nconnection");
        assert_eq!(info.find("rows="), warn.find("broken"));
        assert_eq!(warn.lines().count(), 1);
        assert!(info.starts_with("12:34:56.789 INFO  WRITE"));
    }
}
