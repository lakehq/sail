use crate::error::{SparkError, SparkResult};
use crate::spark::connect as sc;

#[allow(dead_code)]
pub(crate) fn sql_to_expression(_sql: &str) -> SparkResult<sc::Expression> {
    Err(SparkError::unsupported("SQL to expression"))
}

#[cfg(test)]
mod tests {
    use super::sql_to_expression;
    use crate::tests::test_gold_set;

    #[test]
    fn test_sql_to_expression() -> Result<(), Box<dyn std::error::Error>> {
        Ok(test_gold_set(
            "tests/gold_data/expression/*.json",
            |sql: String| Ok(sql_to_expression(&sql)?),
        )?)
    }
}
