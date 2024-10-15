use crate::driver::client::DriverHandle;

pub(super) struct WorkerState {
    driver: DriverHandle,
}

impl WorkerState {
    pub fn new(driver: DriverHandle) -> Self {
        Self { driver }
    }
}
