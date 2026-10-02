use std::fmt;
use std::sync::Arc;

use aes::cipher::block_padding::Pkcs7;
use aes::cipher::consts::U12;
use aes::cipher::{BlockDecrypt, BlockEncrypt, BlockEncryptMut, KeyIvInit};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::rand_core::{OsRng, RngCore};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, Nonce};
use cbc::cipher::BlockDecryptMut;
use datafusion::arrow::array::BinaryBuilder;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_binary_array, as_string_array};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err, internal_err,
};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

pub type Aes192Gcm = AesGcm<Aes192, U12>;

pub fn encryption_name_to_mode(mode: &str, padding: &str) -> Result<EncryptionMode> {
    // Java equalsIgnoreCase also accepts the Kelvin sign and long s in PKCS.
    match (
        mode.to_lowercase().to_uppercase().as_str(),
        padding.to_lowercase().to_uppercase().as_str(),
    ) {
        ("GCM", "NONE" | "DEFAULT") => Ok(EncryptionMode::GCM),
        ("CBC", "PKCS" | "DEFAULT") => Ok(EncryptionMode::CBC),
        ("ECB", "PKCS" | "DEFAULT") => Ok(EncryptionMode::ECB),
        _ => exec_err!("Unsupported AES mode {mode} with padding {padding}"),
    }
}

pub fn generate_iv(mode: &EncryptionMode) -> Vec<u8> {
    match &mode {
        EncryptionMode::ECB => Vec::new(),
        EncryptionMode::GCM => {
            let mut iv = [0u8; 12];
            OsRng.fill_bytes(&mut iv);
            iv.to_vec()
        }
        EncryptionMode::CBC => {
            let mut iv = [0u8; 16];
            OsRng.fill_bytes(&mut iv);
            iv.to_vec()
        }
    }
}

#[expect(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum EncryptionMode {
    GCM,
    CBC,
    ECB,
}

impl fmt::Display for EncryptionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncryptionMode::GCM => write!(f, "GCM"),
            EncryptionMode::CBC => write!(f, "CBC"),
            EncryptionMode::ECB => write!(f, "ECB"),
        }
    }
}

fn invoke_aes(
    args: ScalarFunctionArgs,
    encrypt: bool,
    null_on_error: bool,
) -> Result<ColumnarValue> {
    let ScalarFunctionArgs {
        mut args,
        number_rows,
        ..
    } = args;
    let name = if encrypt {
        "aes_encrypt"
    } else {
        "aes_decrypt"
    };
    let count = if encrypt { 6 } else { 5 };
    if args.len() < 2 || args.len() > count {
        return exec_err!(
            "Spark `{name}` function requires 2 to {count} arguments, got {}",
            args.len()
        );
    }
    let is_scalar = number_rows == 1
        && args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    while args.len() < count {
        let default = match args.len() {
            2 => "GCM",
            3 => "DEFAULT",
            _ => "",
        };
        args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            default.into(),
        ))));
    }
    let arrays = args
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            let data_type = if matches!(index, 2 | 3) {
                DataType::Utf8
            } else {
                DataType::Binary
            };
            arg.cast_to(&data_type, None)?
                .into_array_of_size(number_rows)
        })
        .collect::<Result<Vec<_>>>()?;
    let inputs = as_binary_array(&arrays[0])?;
    let keys = as_binary_array(&arrays[1])?;
    let modes = as_string_array(&arrays[2])?;
    let paddings = as_string_array(&arrays[3])?;
    let fifth = as_binary_array(&arrays[4])?;
    let aads = as_binary_array(&arrays[count - 1])?;
    let mut output = BinaryBuilder::new();
    for row in 0..number_rows {
        if arrays.iter().any(|array| array.is_null(row)) {
            output.append_null();
            continue;
        }
        let result = if encrypt {
            encrypt_value(
                inputs.value(row),
                keys.value(row),
                modes.value(row),
                paddings.value(row),
                fifth.value(row),
                aads.value(row),
            )
        } else {
            decrypt_value(
                inputs.value(row),
                keys.value(row),
                modes.value(row),
                paddings.value(row),
                aads.value(row),
            )
        };
        match result {
            Ok(value) => output.append_value(value),
            Err(_) if null_on_error => output.append_null(),
            Err(error) => return Err(error),
        }
    }
    let output = output.finish();
    if is_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &output, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

fn encrypt_value(
    expr: &[u8],
    key: &[u8],
    mode_name: &str,
    padding: &str,
    iv: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>> {
    if !matches!(key.len(), 16 | 24 | 32) {
        return exec_err!(
            "Spark `aes_encrypt`: Key length must be 16, 24, or 32 bytes, got {}",
            key.len()
        );
    }
    let mode = encryption_name_to_mode(mode_name, padding)?;
    let iv = if iv.is_empty() {
        generate_iv(&mode)
    } else {
        iv.to_vec()
    };
    let iv_length = match mode {
        EncryptionMode::ECB => 0,
        EncryptionMode::CBC => 16,
        EncryptionMode::GCM => 12,
    };
    if iv.len() != iv_length {
        return exec_err!(
            "Spark `aes_encrypt`: IV must be {iv_length} bytes long for {mode} mode, got {}",
            iv.len()
        );
    }
    let aad = (!aad.is_empty()).then_some(aad);
    if aad.is_some() && !matches!(mode, EncryptionMode::GCM) {
        return exec_err!("Spark `aes_encrypt`: AAD is only supported for GCM mode");
    }
    let ciphertext = match &mode {
        EncryptionMode::ECB => {
            match key.len() {
                16 => Aes128::new_from_slice(key)
                    .map(|cipher| cipher.encrypt_padded_vec::<Pkcs7>(expr)),
                24 => Aes192::new_from_slice(key)
                    .map(|cipher| cipher.encrypt_padded_vec::<Pkcs7>(expr)),
                32 => Aes256::new_from_slice(key)
                    .map(|cipher| cipher.encrypt_padded_vec::<Pkcs7>(expr)),
                _ => return exec_err!("Spark `aes_encrypt`: Invalid AES key length"),
            }
            .map_err(|e| exec_datafusion_err!("Spark `aes_encrypt`: ECB Encryption error: {e}"))
        }
        EncryptionMode::GCM => {
            let nonce = Nonce::from_slice(&iv);
            let result = match key.len() {
                16 => {
                    let cipher = Aes128Gcm::new_from_slice(key).map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_encrypt`: Error creating AES-128 cipher: {e}"
                        )
                    })?;
                    let result = match aad {
                        Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                        None => cipher.encrypt(nonce, expr),
                    }
                    .map_err(|e| {
                        exec_datafusion_err!("Spark `aes_encrypt`: GCM Encryption error: {e}")
                    })?;
                    Ok(result)
                }
                24 => {
                    let cipher = Aes192Gcm::new_from_slice(key).map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_encrypt`: Error creating AES-192 cipher: {e}"
                        )
                    })?;
                    let result = match aad {
                        Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                        None => cipher.encrypt(nonce, expr),
                    }
                    .map_err(|e| {
                        exec_datafusion_err!("Spark `aes_encrypt`: GCM Encryption error: {e}")
                    })?;
                    Ok(result)
                }
                32 => {
                    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_encrypt`: Error creating AES-256 cipher: {e}"
                        )
                    })?;
                    let result = match aad {
                        Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                        None => cipher.encrypt(nonce, expr),
                    }
                    .map_err(|e| {
                        exec_datafusion_err!("Spark `aes_encrypt`: GCM Encryption error: {e}")
                    })?;
                    Ok(result)
                }
                other => exec_err!(
                    "Spark `aes_encrypt`: Key length must be 16, 24, or 32 bytes, got {other}"
                ),
            }
            .map_err(|e| exec_datafusion_err!("Spark `aes_encrypt`: GCM Encryption error: {e}"))?;
            let mut ciphertext = iv.to_vec();
            ciphertext.extend_from_slice(&result);
            Ok::<Vec<u8>, DataFusionError>(ciphertext)
        }
        EncryptionMode::CBC => {
            let result = match key.len() {
                16 => cbc::Encryptor::<Aes128>::new_from_slices(key, &iv)
                    .map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_encrypt`: Error creating AES-128 cipher: {e}"
                        )
                    })
                    .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                24 => cbc::Encryptor::<Aes192>::new_from_slices(key, &iv)
                    .map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_encrypt`: Error creating AES-192 cipher: {e}"
                        )
                    })
                    .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                32 => cbc::Encryptor::<Aes256>::new_from_slices(key, &iv)
                    .map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_encrypt`: Error creating AES-256 cipher: {e}"
                        )
                    })
                    .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                other => exec_err!(
                    "Spark `aes_encrypt`: Key length must be 16, 24, or 32 bytes, got {other}"
                ),
            }?;
            let mut ciphertext = iv.to_vec();
            ciphertext.extend_from_slice(&result);
            Ok(ciphertext)
        }
    }?;
    Ok(ciphertext)
}

fn decrypt_value(
    expr: &[u8],
    key: &[u8],
    mode_name: &str,
    padding: &str,
    aad: &[u8],
) -> Result<Vec<u8>> {
    if !matches!(key.len(), 16 | 24 | 32) {
        return exec_err!(
            "Spark `aes_decrypt`: Key length must be 16, 24, or 32 bytes, got {}",
            key.len()
        );
    }
    let mode = encryption_name_to_mode(mode_name, padding)?;
    // Spark ignores AAD for ECB decryption, but rejects it for CBC.
    if !aad.is_empty() && matches!(mode, EncryptionMode::CBC) {
        return exec_err!("Spark `aes_decrypt`: AAD is only supported for GCM mode");
    }
    let aad = (!aad.is_empty()).then_some(aad);
    let result = match &mode {
        // Spark returns empty bytes when there are no ECB blocks to decrypt.
        EncryptionMode::ECB if expr.is_empty() => Ok(Vec::new()),
        EncryptionMode::ECB => {
            let decrypted = match key.len() {
                16 => Aes128::new_from_slice(key)
                    .map(|cipher| cipher.decrypt_padded_vec::<Pkcs7>(expr)),
                24 => Aes192::new_from_slice(key)
                    .map(|cipher| cipher.decrypt_padded_vec::<Pkcs7>(expr)),
                32 => Aes256::new_from_slice(key)
                    .map(|cipher| cipher.decrypt_padded_vec::<Pkcs7>(expr)),
                other => {
                    return exec_err!(
                        "Spark `aes_decrypt`: Key length must be 16, 24, or 32 bytes, got {other}"
                    );
                }
            }
            .map_err(|e| exec_datafusion_err!("Spark `aes_decrypt`: ECB Decryption error: {e}"))?;
            decrypted
                .map_err(|e| exec_datafusion_err!("Spark `aes_decrypt`: ECB Decryption error: {e}"))
        }
        EncryptionMode::GCM => {
            // iv is prepended to the ciphertext
            let (iv, expr) = expr.split_at_checked(12).ok_or_else(|| {
                exec_datafusion_err!(
                    "Spark `aes_decrypt`: Input must be at least 12 bytes long for GCM mode, got {}",
                    expr.len()
                )
            })?;
            let nonce = Nonce::from_slice(iv);
            let decrypted = match key.len() {
                16 => {
                    let cipher = Aes128Gcm::new_from_slice(key).map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_decrypt`: Error creating AES-128 cipher: {e}"
                        )
                    })?;
                    let result = match aad {
                        Some(aad) => cipher.decrypt(nonce, Payload { msg: expr, aad }),
                        None => cipher.decrypt(nonce, expr),
                    }
                    .map_err(|e| {
                        exec_datafusion_err!("Spark `aes_decrypt`: GCM Decryption error: {e}")
                    })?;
                    Ok(result)
                }
                24 => {
                    let cipher = Aes192Gcm::new_from_slice(key).map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_decrypt`: Error creating AES-192 cipher: {e}"
                        )
                    })?;
                    let result = match aad {
                        Some(aad) => cipher.decrypt(nonce, Payload { msg: expr, aad }),
                        None => cipher.decrypt(nonce, expr),
                    }
                    .map_err(|e| {
                        exec_datafusion_err!("Spark `aes_decrypt`: GCM Decryption error: {e}")
                    })?;
                    Ok(result)
                }
                32 => {
                    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| {
                        exec_datafusion_err!(
                            "Spark `aes_decrypt`: Error creating AES-256 cipher: {e}"
                        )
                    })?;
                    let result = match aad {
                        Some(aad) => cipher.decrypt(nonce, Payload { msg: expr, aad }),
                        None => cipher.decrypt(nonce, expr),
                    }
                    .map_err(|e| {
                        exec_datafusion_err!("Spark `aes_decrypt`: GCM Decryption error: {e}")
                    })?;
                    Ok(result)
                }
                other => exec_err!(
                    "Spark `aes_decrypt`: Key length must be 16, 24, or 32 bytes, got {other}"
                ),
            }
            .map_err(|e| exec_datafusion_err!("Spark `aes_decrypt`: GCM Decryption error: {e}"))?;
            Ok::<Vec<u8>, DataFusionError>(decrypted)
        }
        EncryptionMode::CBC => {
            // iv is prepended to the ciphertext
            let (iv, expr) = expr.split_at_checked(16).ok_or_else(|| {
                exec_datafusion_err!(
                    "Spark `aes_decrypt`: Input must be at least 16 bytes long for CBC mode, got {}",
                    expr.len()
                )
            })?;
            // Spark also accepts CBC input containing only the IV.
            if expr.is_empty() {
                return Ok(Vec::new());
            }
            let decrypted = match key.len() {
                16 => {
                    let decryptor =
                        cbc::Decryptor::<Aes128>::new_from_slices(key, iv).map_err(|e| {
                            exec_datafusion_err!(
                                "Spark `aes_decrypt`: Error creating AES-128 cipher: {e}"
                            )
                        })?;
                    let result = decryptor
                        .decrypt_padded_vec_mut::<Pkcs7>(expr)
                        .map_err(|e| {
                            exec_datafusion_err!("Spark `aes_decrypt`: CBC Decryption error: {e}")
                        })?;
                    Ok(result)
                }
                24 => {
                    let decryptor =
                        cbc::Decryptor::<Aes192>::new_from_slices(key, iv).map_err(|e| {
                            exec_datafusion_err!(
                                "Spark `aes_decrypt`: Error creating AES-192 cipher: {e}"
                            )
                        })?;
                    let result = decryptor
                        .decrypt_padded_vec_mut::<Pkcs7>(expr)
                        .map_err(|e| {
                            exec_datafusion_err!("Spark `aes_decrypt`: CBC Decryption error: {e}")
                        })?;
                    Ok(result)
                }
                32 => {
                    let decryptor =
                        cbc::Decryptor::<Aes256>::new_from_slices(key, iv).map_err(|e| {
                            exec_datafusion_err!(
                                "Spark `aes_decrypt`: Error creating AES-256 cipher: {e}"
                            )
                        })?;
                    let result = decryptor
                        .decrypt_padded_vec_mut::<Pkcs7>(expr)
                        .map_err(|e| {
                            exec_datafusion_err!("Spark `aes_decrypt`: CBC Decryption error: {e}")
                        })?;
                    Ok(result)
                }
                other => exec_err!(
                    "Spark `aes_decrypt`: Key length must be 16, 24, or 32 bytes, got {other}"
                ),
            }?;
            Ok(decrypted)
        }
    }?;
    Ok(result)
}

/// Arguments
///   - `expr`: The BINARY expression to be encrypted.
///   - `key`: A BINARY expression. The key to be used to encrypt expr. It must be 16, 24, or 32 bytes long.
///     The algorithm depends on the length of the `key`:
///     - 16 bytes: AES-128
///     - 24 bytes: AES-192
///     - 32 bytes: AES-256
///   - `mode`: An optional STRING expression describing the encryption mode.
///     `mode` must be one of (case-insensitive):
///     - `GCM`: Use Galois/Counter Mode (GCM). This is the default.
///     - `CBC`: Use Cipher-Block Chaining (CBC) mode.
///     - `ECB`: Use Electronic CodeBook (ECB) mode.
///   - `padding`: An optional STRING expression describing how encryption pads the input to the AES block size.
///     `padding` must be one of (case-insensitive):
///     - `NONE`: Uses no padding. Valid only for `GCM`.
///     - `DEFAULT`: Uses `NONE` for `GCM` and `PKCS` for `ECB`, and `CBC` mode.
///     - `PKCS`: Uses Public Key Cryptography Standards (PKCS) padding. Valid only for `ECB` and `CBC`.
///       PKCS padding adds between 1 and 16 bytes to pad expr to a multiple of the AES block size.
///       The value of each pad byte is the number of bytes being padded.
///   - `iv`: An optional STRING expression providing an initialization vector (IV) for GCM or CBC modes.
///     `iv`, when specified, must be 12-bytes long for GCM and 16 bytes for CBC.
///     If not provided, a random vector will be generated and prepended to the output.
///   - `aad`: An optional STRING expression providing authenticated additional data (AAD) in GCM mode.
///     Optional additional authenticated data (AAD) is only supported for GCM.
///     If provided for encryption, the identical AAD value must be provided for decryption.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAESEncrypt {
    signature: Signature,
}

impl Default for SparkAESEncrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAESEncrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkAESEncrypt {
    fn name(&self) -> &str {
        "spark_aes_encrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `AesEncrypt` is `RuntimeReplaceable` over a `StaticInvoke` that leaves
    /// `returnNullable` at its `true` default, so `nullable = true` whatever the arguments.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/objects/objects.scala#L334>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 6 {
            return exec_err!(
                "Spark `aes_encrypt` function requires 2 to 6 arguments, got {}",
                args.arg_fields.len()
            );
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_aes(args, true, false)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAESDecrypt {
    signature: Signature,
}

impl Default for SparkAESDecrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAESDecrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkAESDecrypt {
    fn name(&self) -> &str {
        "spark_aes_decrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `AesDecrypt` is `RuntimeReplaceable` over a `StaticInvoke` that leaves
    /// `returnNullable` at its `true` default, so `nullable = true` whatever the arguments.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/objects/objects.scala#L334>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 5 {
            return exec_err!(
                "Spark `aes_decrypt` function requires 2 to 5 arguments, got {}",
                args.arg_fields.len()
            );
        }
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_aes(args, false, false)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryAESEncrypt {
    signature: Signature,
}

impl Default for SparkTryAESEncrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryAESEncrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryAESEncrypt {
    fn name(&self) -> &str {
        "spark_try_aes_encrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryEval.nullable = true`, unconditional; `try_aes_encrypt` has no expression of
    /// its own and follows its `TryAesDecrypt` sibling, which wraps the strict call in `TryEval`.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_aes(args, true, true)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryAESDecrypt {
    signature: Signature,
}

impl Default for SparkTryAESDecrypt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryAESDecrypt {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryAESDecrypt {
    fn name(&self) -> &str {
        "spark_try_aes_decrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TryAesDecrypt` is `RuntimeReplaceable` over `TryEval(AesDecrypt(...))`, whose
    /// `nullable = true` is unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/TryEval.scala#L50>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_aes(args, false, true)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::config::ConfigOptions;

    use super::*;

    fn decrypt_args(expr: Vec<u8>, mode: &str) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Binary(Some(expr))),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(b"0000111122223333".to_vec()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(mode.to_string()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("expr", DataType::Binary, true)),
                Arc::new(Field::new("key", DataType::Binary, true)),
                Arc::new(Field::new("mode", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Binary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn test_aes_decrypt_input_shorter_than_iv() -> Result<()> {
        // The IV is prepended to the ciphertext: 12 bytes for GCM and 16 bytes for CBC.
        for (mode, len) in [("GCM", 0), ("GCM", 11), ("CBC", 0), ("CBC", 15)] {
            let result = SparkAESDecrypt::new().invoke_with_args(decrypt_args(vec![0; len], mode));
            assert!(result.is_err(), "{mode} input of {len} byte(s)");

            let result =
                SparkTryAESDecrypt::new().invoke_with_args(decrypt_args(vec![0; len], mode))?;
            assert!(
                matches!(result, ColumnarValue::Scalar(ScalarValue::Binary(None))),
                "{mode} input of {len} byte(s)"
            );
        }
        Ok(())
    }
}
