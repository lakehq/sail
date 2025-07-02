mod log_data;

mod datafusion {
    use ::datafusion::common::stats::Statistics;

    use super::*;

    impl EagerSnapshot {
        /// Provide table level statistics to Datafusion
        pub fn datafusion_table_statistics(&self) -> Option<Statistics> {
            self.log_data().statistics()
        }
    }
}
