use anyhow::Result;

use super::Writer;
use crate::{
    events::{AllEvents, SimpleMessageGroup},
    settings::ClickhouseSettings,
};

pub struct ClickhouseWriter {
    pub config: ClickhouseSettings,
}

impl Writer for ClickhouseWriter {
    fn write(&self, event: AllEvents) -> Result<()> {
        let smg = SimpleMessageGroup::from(event);

        smg.0.iter().for_each(|msg| println!("{:?}", msg));
        Ok(())
    }
}

impl ClickhouseWriter {
    pub fn new(config: ClickhouseSettings) -> ClickhouseWriter {
        ClickhouseWriter { config }
    }
    pub async fn start(&self) {}
}
