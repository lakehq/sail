use datafusion_expr_common::signature::Signature;

#[derive(Debug)]
pub struct SparkLuhnCheck {
    signature: Signature,
}