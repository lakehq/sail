use iana_time_zone::get_timezone;

use crate::error::{CommonError, CommonResult};

pub fn get_system_timezone() -> CommonResult<String> {
    get_timezone().map_err(|e| CommonError::invalid(format!("failed to get system timezone: {e}")))
}
