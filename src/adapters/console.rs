use anyhow::Result;
use colored::{ColoredString, Colorize};

use super::Writer;
use crate::events::{AllEvents, SimpleMessage, SimpleMessageGroup};

pub struct ConsoleWriter;

impl ConsoleWriter {
    pub fn new() -> Self {
        Self {}
    }
    fn print_message(message: &SimpleMessage) {
        println!(
            "[{}] [{}] {}: {}",
            message.timestamp.format("%Y-%m-%d %H:%M:%S%.3f %Z"),
            message.channel.bright_red(),
            ColoredString::from(message.username.clone()),
            message.text
        );
    }
}

impl Writer for ConsoleWriter {
    fn write(&self, event: AllEvents) -> Result<()> {
        let smg = SimpleMessageGroup::from(event);

        smg.0.iter().for_each(ConsoleWriter::print_message);
        Ok(())
    }
}
