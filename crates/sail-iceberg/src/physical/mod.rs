// SPDX-License-Identifier: Apache-2.0

mod row_level_planner;
pub mod table_scan_planner;

pub use table_scan_planner::IcebergPhysicalPlanner;
