use crate::error::{SparkError, SparkResult};
use crate::spark::connect as sc;

#[allow(dead_code)]
pub(crate) fn sql_to_plan(_sql: &str) -> SparkResult<sc::Plan> {
    Err(SparkError::unsupported("SQL to plan"))
}

#[cfg(test)]
mod tests {
    use super::sql_to_plan;
    use crate::tests::test_gold_set;

    #[test]
    fn test_sql_to_plan() -> Result<(), Box<dyn std::error::Error>> {
        Ok(test_gold_set(
            "tests/gold_data/plan/*.json",
            |sql: String| Ok(sql_to_plan(&sql)?),
        )?)
    }
}
