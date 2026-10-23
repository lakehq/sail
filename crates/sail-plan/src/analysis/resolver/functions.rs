use std::sync::Arc;

use datafusion_expr::ScalarUDF;

use crate::config::PlanConfig;

/// Direct kernel handles. No backend expression is built to discover a function.
pub(super) fn scalar_kernel(name: &str, config: &PlanConfig) -> Option<Arc<ScalarUDF>> {
    if let Some(kind) = sail_function::scalar::jev::JevKind::from_name(name) {
        return Some(Arc::new(sail_function::scalar::jev::jev_udf(kind)));
    }
    use sail_function::scalar::xml::xpath_typed::{XpathTyped, XpathTypedKind};
    Some(Arc::new(match name {
        "input_file_block_length" => ScalarUDF::from(sail_common_datafusion::input_file::InputFileBlockLengthFunc::new()),
        "input_file_block_start" => ScalarUDF::from(sail_common_datafusion::input_file::InputFileBlockStartFunc::new()),
        "monotonically_increasing_id" => ScalarUDF::from(sail_function::scalar::misc::monotonically_increasing_id::SparkMonotonicallyIncreasingId::new()),
        "spark_partition_id" => ScalarUDF::from(sail_function::scalar::misc::spark_partition_id::SparkPartitionId::new()),
        "bit_count" => return Some(datafusion_spark::function::bitwise::bit_count()),
        "bit_get" => return Some(datafusion_spark::function::bitwise::bit_get()),
        "bitmap_count" => return Some(datafusion_spark::function::bitmap::bitmap_count()),
        "bitwise_not" => return Some(datafusion_spark::function::bitwise::bitwise_not()),
        "crc32" => return Some(datafusion_spark::function::hash::crc32()),
        "expm1" => return Some(datafusion_spark::function::math::expm1()),
        "getbit" => return Some(datafusion_spark::function::bitwise::bit_get()),
        "hex" => return Some(datafusion_spark::function::math::hex()),
        "luhn_check" => return Some(datafusion_spark::function::string::luhn_check()),
        "sha" => return Some(datafusion_spark::function::hash::sha1()),
        "sha1" => return Some(datafusion_spark::function::hash::sha1()),
        "shuffle" => return Some(datafusion_spark::function::array::shuffle()),
        "width_bucket" => return Some(datafusion_spark::function::math::width_bucket()),
        "st_asbinary" => {
            ScalarUDF::from(sail_function::scalar::geo::st_asbinary::StAsBinary::new())
        }
        "st_geomfromwkb" => {
            ScalarUDF::from(sail_function::scalar::geo::st_geomfromwkb::StGeomFromWKB::new())
        }
        "st_geogfromwkb" => {
            ScalarUDF::from(sail_function::scalar::geo::st_geogfromwkb::StGeogFromWKB::new())
        }
        "xpath" => ScalarUDF::from(sail_function::scalar::xml::xpath::Xpath::new()),
        "xpath_boolean" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Boolean)),
        "xpath_double" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Double)),
        "xpath_float" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Float)),
        "xpath_int" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Int)),
        "xpath_long" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Long)),
        "xpath_number" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Number)),
        "xpath_short" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::Short)),
        "xpath_string" => ScalarUDF::from(XpathTyped::new(XpathTypedKind::String)),
        "abs" => ScalarUDF::from(sail_function::scalar::math::spark_abs::SparkAbs::new(
            config.ansi_mode,
        )),
        "aes_decrypt" => {
            ScalarUDF::from(sail_function::scalar::misc::spark_aes::SparkAESDecrypt::new())
        }
        "aes_encrypt" => {
            ScalarUDF::from(sail_function::scalar::misc::spark_aes::SparkAESEncrypt::new())
        }
        "array" => ScalarUDF::from(sail_function::scalar::array::spark_array::SparkArray::new()),
        "array_concat" => {
            ScalarUDF::from(sail_function::scalar::collection::spark_concat::SparkConcat::new())
        }
        "array_intersect" => {
            ScalarUDF::from(sail_function::scalar::array::array_intersect::ArrayIntersect::new())
        }
        "array_join" => ScalarUDF::from(datafusion_functions_nested::string::ArrayToString::new()),
        "array_max" => {
            ScalarUDF::from(sail_function::scalar::array::spark_array_min_max::ArrayMax::new())
        }
        "array_min" => {
            ScalarUDF::from(sail_function::scalar::array::spark_array_min_max::ArrayMin::new())
        }
        "array_position" => {
            ScalarUDF::from(sail_function::scalar::array::array_position::SparkArrayPosition::new())
        }
        "base64" => {
            ScalarUDF::from(sail_function::scalar::string::spark_base64::SparkBase64::new())
        }
        "bround" => ScalarUDF::from(sail_function::scalar::math::spark_bround::SparkBRound::new()),
        "concat" => {
            ScalarUDF::from(sail_function::scalar::collection::spark_concat::SparkConcat::new())
        }
        "concat_ws" => {
            ScalarUDF::from(sail_function::scalar::string::spark_concat_ws::SparkConcatWs::new())
        }
        "conv" => ScalarUDF::from(sail_function::scalar::math::spark_conv::SparkConv::new()),
        "decode" => {
            ScalarUDF::from(sail_function::scalar::string::spark_encode_decode::SparkDecode::new())
        }
        "elt" => ScalarUDF::from(datafusion_spark::function::string::elt::SparkElt::new()),
        "encode" => {
            ScalarUDF::from(sail_function::scalar::string::spark_encode_decode::SparkEncode::new())
        }
        "format_number" => {
            ScalarUDF::from(sail_function::scalar::string::format_number::FormatNumber::new())
        }
        "format_string" => ScalarUDF::from(
            datafusion_spark::function::string::format_string::FormatStringFunc::new(),
        ),
        "hash" => ScalarUDF::from(
            sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash::new(),
        ),
        "hll_sketch_estimate" => ScalarUDF::from(
            sail_function::scalar::misc::hll_sketch::HllSketchEstimateFunction::new(),
        ),
        "is_variant_null" => ScalarUDF::from(
            sail_function::scalar::variant::spark_is_variant_null::SparkIsVariantNullUdf::new(),
        ),
        "last_day" => {
            ScalarUDF::from(sail_function::scalar::datetime::spark_last_day::SparkLastDay::new())
        }
        "levenshtein" => {
            ScalarUDF::from(sail_function::scalar::string::levenshtein::Levenshtein::new())
        }
        "make_dt_interval" => ScalarUDF::from(
            datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval::new(),
        ),
        "make_interval" => ScalarUDF::from(
            datafusion_spark::function::datetime::make_interval::SparkMakeInterval::new(),
        ),
        "make_time" => {
            ScalarUDF::from(sail_function::scalar::datetime::spark_make_time::SparkMakeTime::new())
        }
        "make_valid_utf8" => {
            ScalarUDF::from(sail_function::scalar::string::make_valid_utf8::MakeValidUtf8::new())
        }
        "make_ym_interval" => ScalarUDF::from(
            sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval::new(),
        ),
        "mask" => ScalarUDF::from(sail_function::scalar::string::spark_mask::SparkMask::new()),
        "parse_json" => ScalarUDF::from(
            sail_function::scalar::variant::spark_parse_json::SparkParseJson::new(false),
        ),
        "parse_url" => ScalarUDF::from(sail_function::scalar::url::parse_url::ParseUrl::new()),
        "printf" => ScalarUDF::from(
            datafusion_spark::function::string::format_string::FormatStringFunc::new(),
        ),
        "quote" => ScalarUDF::from(sail_function::scalar::string::spark_quote::SparkQuote::new()),
        "raise_error" => {
            ScalarUDF::from(sail_function::scalar::misc::raise_error::RaiseError::new())
        }
        "rand" => ScalarUDF::from(sail_function::scalar::math::random::Random::new()),
        "randn" => ScalarUDF::from(sail_function::scalar::math::randn::Randn::new()),
        "random" => ScalarUDF::from(sail_function::scalar::math::random::Random::new()),
        "random_poisson" => {
            ScalarUDF::from(sail_function::scalar::math::rand_poisson::RandPoisson::new())
        }
        "randstr" => ScalarUDF::from(sail_function::scalar::string::randstr::Randstr::new()),
        "regexp_count" => {
            ScalarUDF::from(datafusion::functions::regex::regexpcount::RegexpCountFunc::new())
        }
        "regexp_extract" => ScalarUDF::from(
            sail_function::scalar::string::spark_regexp_extract_all::SparkRegexpExtract::new(),
        ),
        "regexp_extract_all" => ScalarUDF::from(
            sail_function::scalar::string::spark_regexp_extract_all::SparkRegexpExtractAll::new(),
        ),
        "reverse" => {
            ScalarUDF::from(sail_function::scalar::collection::spark_reverse::SparkReverse::new())
        }
        "schema_of_csv" => ScalarUDF::from(sail_function::scalar::csv::SparkSchemaOfCsv::new()),
        "schema_of_json" => ScalarUDF::from(sail_function::scalar::json::SparkSchemaOfJson::new()),
        "schema_of_variant" => ScalarUDF::from(
            sail_function::scalar::variant::spark_schema_of_variant::SparkSchemaOfVariantUdf::new(),
        ),
        "sentences" => {
            ScalarUDF::from(sail_function::scalar::string::spark_sentences::SparkSentences::new())
        }
        "sign" => ScalarUDF::from(sail_function::scalar::math::spark_signum::SparkSignum::new()),
        "signum" => ScalarUDF::from(sail_function::scalar::math::spark_signum::SparkSignum::new()),
        "soundex" => ScalarUDF::from(sail_function::scalar::string::soundex::Soundex::new()),
        "split" => ScalarUDF::from(sail_function::scalar::string::spark_split::SparkSplit::new()),
        "theta_difference" => ScalarUDF::from(
            sail_function::scalar::misc::theta_sketch::ThetaDifferenceFunction::new(),
        ),
        "theta_intersection" => ScalarUDF::from(
            sail_function::scalar::misc::theta_sketch::ThetaIntersectionFunction::new(),
        ),
        "theta_sketch_estimate" => ScalarUDF::from(
            sail_function::scalar::misc::theta_sketch::ThetaSketchEstimateFunction::new(),
        ),
        "time_diff" => {
            ScalarUDF::from(sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff::new())
        }
        "time_trunc" => ScalarUDF::from(
            sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc::new(),
        ),
        "to_binary" => {
            ScalarUDF::from(sail_function::scalar::string::spark_to_binary::SparkToBinary::new())
        }
        "to_number" => ScalarUDF::from(
            sail_function::scalar::string::spark_to_number::SparkToNumber::new(false),
        ),
        "to_variant_object" => ScalarUDF::from(
            sail_function::scalar::variant::spark_to_variant_object::SparkToVariantObjectUdf::new(),
        ),
        "try_add" => {
            ScalarUDF::from(sail_function::scalar::math::spark_try_add::SparkTryAdd::new())
        }
        "try_aes_decrypt" => {
            ScalarUDF::from(sail_function::scalar::misc::spark_aes::SparkTryAESDecrypt::new())
        }
        "try_aes_encrypt" => {
            ScalarUDF::from(sail_function::scalar::misc::spark_aes::SparkTryAESEncrypt::new())
        }
        "try_divide" => {
            ScalarUDF::from(sail_function::scalar::math::spark_try_div::SparkTryDiv::new())
        }
        "try_make_interval" => ScalarUDF::from(
            datafusion_spark::function::datetime::make_interval::SparkMakeInterval::new(),
        ),
        "try_mod" => {
            ScalarUDF::from(sail_function::scalar::math::spark_try_mod::SparkTryMod::new())
        }
        "try_multiply" => {
            ScalarUDF::from(sail_function::scalar::math::spark_try_mult::SparkTryMult::new())
        }
        "try_parse_json" => ScalarUDF::from(
            sail_function::scalar::variant::spark_parse_json::SparkParseJson::new(true),
        ),
        "try_parse_url" => ScalarUDF::from(
            sail_function::scalar::url::spark_try_parse_url::SparkTryParseUrl::new(),
        ),
        "try_subtract" => ScalarUDF::from(
            sail_function::scalar::math::spark_try_subtract::SparkTrySubtract::new(),
        ),
        "try_to_binary" => {
            ScalarUDF::from(sail_function::scalar::string::spark_to_binary::SparkTryToBinary::new())
        }
        "try_to_number" => ScalarUDF::from(
            sail_function::scalar::string::spark_to_number::SparkToNumber::new(true),
        ),
        "try_url_decode" => {
            ScalarUDF::from(datafusion_spark::function::url::try_url_decode::TryUrlDecode::new())
        }
        "try_variant_get" => ScalarUDF::from(
            sail_function::scalar::variant::spark_variant_get::SparkVariantGet::new(true),
        ),
        "unbase64" => {
            ScalarUDF::from(sail_function::scalar::string::spark_base64::SparkUnbase64::new())
        }
        "unhex" => ScalarUDF::from(sail_function::scalar::math::spark_unhex::SparkUnHex::new()),
        "uniform" => {
            ScalarUDF::from(sail_function::scalar::math::spark_uniform::SparkUniform::new())
        }
        "url_decode" => {
            ScalarUDF::from(datafusion_spark::function::url::url_decode::UrlDecode::new())
        }
        "url_encode" => {
            ScalarUDF::from(datafusion_spark::function::url::url_encode::UrlEncode::new())
        }
        "variant_get" => ScalarUDF::from(
            sail_function::scalar::variant::spark_variant_get::SparkVariantGet::new(false),
        ),
        "variant_to_json" => ScalarUDF::from(
            sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf::new(),
        ),
        "vector_cosine_similarity" => ScalarUDF::from(
            sail_function::scalar::vector::cosine_similarity::VectorCosineSimilarity::new(),
        ),
        "vector_inner_product" => {
            ScalarUDF::from(sail_function::scalar::vector::inner_product::VectorInnerProduct::new())
        }
        "vector_l2_distance" => {
            ScalarUDF::from(sail_function::scalar::vector::l2_distance::VectorL2Distance::new())
        }
        "vector_norm" => ScalarUDF::from(sail_function::scalar::vector::norm::VectorNorm::new()),
        "vector_normalize" => {
            ScalarUDF::from(sail_function::scalar::vector::normalize::VectorNormalize::new())
        }
        "version" => ScalarUDF::from(sail_function::scalar::misc::version::SparkVersion::new()),
        "xxhash64" => {
            ScalarUDF::from(datafusion_spark::function::hash::xxhash64::SparkXxhash64::new())
        }
        "year" => ScalarUDF::from(sail_function::scalar::datetime::spark_year::SparkYear::new()),
        _ => return None,
    }))
}

/// Backend kernels are handles only; argument and result typing stays in native analysis.
pub(super) fn default_scalar_kernel(name: &str) -> Option<Arc<ScalarUDF>> {
    static FUNCTIONS: std::sync::LazyLock<std::collections::HashMap<String, Arc<ScalarUDF>>> =
        std::sync::LazyLock::new(|| {
            let mut functions = std::collections::HashMap::new();
            for function in datafusion::functions::all_default_functions()
                .into_iter()
                .chain(datafusion::functions_nested::all_default_nested_functions())
            {
                functions.insert(function.name().to_string(), Arc::clone(&function));
                for alias in function.aliases() {
                    functions.insert(alias.clone(), Arc::clone(&function));
                }
            }
            functions
        });
    FUNCTIONS.get(name).cloned()
}
