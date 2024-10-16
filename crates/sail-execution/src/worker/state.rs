use crate::driver::DriverHandle;

pub struct WorkerState {
    driver: DriverHandle,
}

impl WorkerState {
    pub fn new(driver: DriverHandle) -> Self {
        Self { driver }
    }
}
