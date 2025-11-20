use indicatif::{ProgressBar, ProgressStyle};
use nu_ansi_term::{Color, Style};
use std::fmt::Display;
use std::io::IsTerminal;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Theme {
    Auto,
    Light,
    Dark,
    Plain,
}

pub struct Ui {
    palette: Palette,
    paint: bool,
    quiet: bool,
    spinner_style: ProgressStyle,
}

impl Ui {
    pub fn new(theme: Theme, quiet: bool) -> Self {
        let stdout_is_tty = std::io::stdout().is_terminal();
        let paint = match theme {
            Theme::Plain => false,
            Theme::Auto => stdout_is_tty,
            Theme::Light | Theme::Dark => stdout_is_tty,
        } && !quiet;

        #[cfg(windows)]
        if paint {
            let _ = nu_ansi_term::enable_ansi_support();
        }

        let palette = match theme {
            Theme::Plain => Palette::plain(),
            Theme::Light => Palette::light(),
            Theme::Dark | Theme::Auto => Palette::dark(),
        };

        let spinner_style = ProgressStyle::with_template("{prefix} {spinner} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏");

        Self {
            palette,
            paint,
            quiet,
            spinner_style,
        }
    }

    pub fn spacer(&self) {
        if !self.quiet {
            println!();
        }
    }

    pub fn section<'a, I, V>(&self, title: &str, rows: I)
    where
        I: IntoIterator<Item = (&'a str, V)>,
        V: Display,
    {
        let rows: Vec<(String, String)> = rows
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        if rows.is_empty() {
            return;
        }

        self.heading(title);
        let key_width = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
        for (key, value) in rows {
            if self.paint {
                println!(
                    "  {} {}",
                    self.palette.key.paint(format!("{key:>key_width$}:")),
                    self.palette.value.paint(value)
                );
            } else {
                println!("  {key:>key_width$}: {value}");
            }
        }
    }

    pub fn list<I>(&self, title: &str, entries: I)
    where
        I: IntoIterator<Item = String>,
    {
        let entries: Vec<String> = entries.into_iter().collect();
        if entries.is_empty() {
            return;
        }
        self.heading(title);
        for entry in entries {
            if self.paint && !self.quiet {
                println!("  {} {entry}", self.palette.bullet.paint("•"));
            } else {
                println!("  - {entry}");
            }
        }
    }

    pub fn info(&self, message: &str) {
        if self.quiet {
            println!("{message}");
            return;
        }
        let prefix = if self.paint {
            self.palette.info.paint(INFO_ICON)
        } else {
            Style::new().paint(INFO_ICON)
        };
        println!("{prefix} {message}");
    }

    pub fn success(&self, message: &str) {
        if self.quiet {
            println!("{message}");
            return;
        }
        let prefix = if self.paint {
            self.palette.success.paint(SUCCESS_ICON)
        } else {
            Style::new().paint(SUCCESS_ICON)
        };
        println!("{prefix} {message}");
    }

    pub fn warn(&self, message: &str) {
        if self.quiet {
            eprintln!("{message}");
            return;
        }
        let prefix = if self.paint {
            self.palette.warn.paint(WARNING_ICON)
        } else {
            Style::new().paint(WARNING_ICON)
        };
        eprintln!("{prefix} {message}");
    }

    pub fn task<'a>(&'a self, label: impl Into<String>) -> TaskGuard<'a> {
        let label = label.into();
        let pb = if self.quiet {
            None
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_style(self.spinner_style.clone());
            let prefix = if self.paint {
                self.palette.info.paint(PROGRESS_ICON).to_string()
            } else {
                PROGRESS_ICON.to_string()
            };
            pb.set_prefix(prefix);
            pb.set_message(label.clone());
            pb.enable_steady_tick(Duration::from_millis(120));
            Some(pb)
        };
        TaskGuard {
            ui: self,
            label,
            start: Instant::now(),
            finished: false,
            pb,
        }
    }

    fn heading(&self, title: &str) {
        if self.quiet {
            println!("{title}");
            return;
        }
        let formatted = format!("{HEADING_ICON} {title}");
        if self.paint {
            println!("{}", self.palette.heading.paint(formatted));
        } else {
            println!("{formatted}");
        }
    }
}

pub struct TaskGuard<'a> {
    ui: &'a Ui,
    label: String,
    start: Instant,
    finished: bool,
    pb: Option<ProgressBar>,
}

impl<'a> TaskGuard<'a> {
    pub fn finish(mut self) -> Duration {
        self.finished = true;
        let elapsed = self.start.elapsed();
        if let Some(pb) = self.pb.take() {
            pb.finish_and_clear();
        }
        elapsed
    }
}

impl<'a> Drop for TaskGuard<'a> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let elapsed = format_duration(self.start.elapsed());
        if let Some(pb) = self.pb.take() {
            pb.abandon_with_message(format!("{} interrupted after {elapsed}", self.label));
        } else {
            self.ui
                .warn(&format!("{} interrupted after {elapsed}", self.label));
        }
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs_f64() >= 1.0 {
        format!("{:.2}s", duration.as_secs_f64())
    } else {
        format!("{:.0}ms", duration.as_secs_f64() * 1_000.0)
    }
}

struct Palette {
    heading: Style,
    key: Style,
    value: Style,
    bullet: Style,
    info: Style,
    success: Style,
    warn: Style,
}

impl Palette {
    fn dark() -> Self {
        Self {
            heading: Style::new().fg(Color::Purple).bold(),
            key: Style::new().fg(Color::LightBlue).bold(),
            value: Style::new().fg(Color::White),
            bullet: Style::new().fg(Color::LightBlue),
            info: Style::new().fg(Color::LightCyan),
            success: Style::new().fg(Color::LightGreen).bold(),
            warn: Style::new().fg(Color::Yellow).bold(),
        }
    }

    fn light() -> Self {
        Self {
            heading: Style::new().fg(Color::Blue).bold(),
            key: Style::new().fg(Color::Black).bold(),
            value: Style::new().fg(Color::Black),
            bullet: Style::new().fg(Color::Blue),
            info: Style::new().fg(Color::Purple),
            success: Style::new().fg(Color::Green).bold(),
            warn: Style::new().fg(Color::Red).bold(),
        }
    }

    fn plain() -> Self {
        Self {
            heading: Style::new(),
            key: Style::new(),
            value: Style::new(),
            bullet: Style::new(),
            info: Style::new(),
            success: Style::new(),
            warn: Style::new(),
        }
    }
}

const HEADING_ICON: &str = "▸";
const SUCCESS_ICON: &str = "✔";
const WARNING_ICON: &str = "⚠";
const INFO_ICON: &str = "ℹ";
const PROGRESS_ICON: &str = "▶";
