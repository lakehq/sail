pub mod location_generator;

use super::WriteOutcome;

pub trait FileWriter {
    fn write(&mut self, _bytes: &[u8]) -> Result<(), String>;
    fn finish(self) -> Result<WriteOutcome, String>;
}
