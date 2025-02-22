use std::any::Any;
use std::fmt;

use aes::cipher::block_padding::Pkcs7;
use aes::cipher::consts::U12;
use aes::cipher::{BlockEncryptMut, KeyIvInit};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm::aead::rand_core::{OsRng, RngCore};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, Nonce};
use cbc::cipher::BlockDecryptMut;
use datafusion::arrow::array::{
    BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
    StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

pub type Aes192Gcm = AesGcm<Aes192, U12>;

/// ECB mode is not supported because it is insecure.
/// We swap ECB for CBC because CBC builds on the foundational block encryption operation that ECB uses
/// and adds important security improvements through its chaining mechanism.
pub fn encryption_name_to_mode(mode: &str) -> Result<EncryptionMode> {
    match mode.trim().to_uppercase().as_str() {
        "" | "GCM" => Ok(EncryptionMode::GCM),
        "CBC" | "ECB" => Ok(EncryptionMode::CBC),
        other => Err(DataFusionError::Plan(format!(
            "Invalid encryption mode, must be one of 'GCM', 'CBC', or 'ECB'. Got {other}"
        ))),
    }
}

pub fn generate_iv(mode: &EncryptionMode) -> Vec<u8> {
    match &mode {
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

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum EncryptionMode {
    GCM,
    CBC,
}

impl fmt::Display for EncryptionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncryptionMode::GCM => write!(f, "GCM"),
            EncryptionMode::CBC => write!(f, "CBC"),
        }
    }
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
///   - `padding`: (USELESS) An optional STRING expression describing how encryption handles padding of the value to key length.
///     `padding` must be one of (case-insensitive):
///     - `NONE`: Uses no padding. Valid only for `GCM`.
///     - `DEFAULT`: Uses `NONE` for `GCM` and `PKCS` for `ECB`, and `CBC` mode.
///     - `PKCS`: Uses Public Key Cryptography Standards (PKCS) padding. Valid only for `ECB` and `CBC`.
///       PKCS padding adds between 1 and key-length number of bytes to pad expr to a multiple of key length.
///       The value of each pad byte is the number of bytes being padded.
///   - `iv`: An optional STRING expression providing an initialization vector (IV) for GCM or CBC modes.
///     `iv`, when specified, must be 12-bytes long for GCM and 16 bytes for CBC.
///     If not provided, a random vector will be generated and prepended to the output.
///   - `aad`: An optional STRING expression providing authenticated additional data (AAD) in GCM mode.
///     Optional additional authenticated data (AAD) is only supported for GCM.
///     If provided for encryption, the identical AAD value must be provided for decryption.
#[derive(Debug)]
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
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

// TODO: Support array batch
impl ScalarUDFImpl for SparkAESEncrypt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_aes_encrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 || arg_types.len() > 6 {
            return exec_err!(
                "Spark `aes_encrypt` function requires 2 to 6 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Binary)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() < 2 || args.len() > 6 {
            return exec_err!(
                "Spark `aes_encrypt` function requires 2 to 6 arguments, got {}",
                args.len()
            );
        }

        let expr = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(expr))) => Ok(expr.as_slice()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => Ok(expr.as_bytes()),
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return exec_err!(
                        "Spark `aes_encrypt`: Expr requires a single value, got {array:?}"
                    );
                }
                match array.data_type() {
                    DataType::Binary => {
                        let array = array.as_any().downcast_ref::<BinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to BinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::BinaryView => {
                        let array = array.as_any().downcast_ref::<BinaryViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::FixedSizeBinary(_) => {
                        let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to FixedSizeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::LargeBinary => {
                        let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::Utf8 => {
                        let array = array.as_any().downcast_ref::<StringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to StringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::LargeUtf8 => {
                        let array = array.as_any().downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to LargeStringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::Utf8View => {
                        let array = array.as_any().downcast_ref::<StringViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Expr to StringViewArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    other => exec_err!("Spark `aes_encrypt`: Expr array must be BINARY or STRING, got array of type {other}")
                }
            }
            other => exec_err!("Spark `aes_encrypt`: Expr must be BINARY or STRING, got {other:?}"),
        }?;

        let key = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(key)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(key))) => Ok(key.as_slice()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(key))) => Ok(key.as_bytes()),
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return exec_err!(
                        "Spark `aes_encrypt`: Key requires a single value, got {array:?}"
                    );
                }
                match array.data_type() {
                    DataType::Binary => {
                        let array = array.as_any().downcast_ref::<BinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to BinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::BinaryView => {
                        let array = array.as_any().downcast_ref::<BinaryViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::FixedSizeBinary(_) => {
                        let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to FixedSizeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::LargeBinary => {
                        let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::Utf8 => {
                        let array = array.as_any().downcast_ref::<StringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to StringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::LargeUtf8 => {
                        let array = array.as_any().downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to LargeStringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::Utf8View => {
                        let array = array.as_any().downcast_ref::<StringViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to StringViewArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    other => exec_err!("Spark `aes_encrypt`: Key array must be BINARY or STRING, got array of type {other}")
                }
            }
            other => exec_err!("Spark `aes_encrypt`: Key must be BINARY, got {other:?}"),
        }?;

        let mode = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(mode)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(mode)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(mode))) => {
                    encryption_name_to_mode(mode)
                }
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return exec_err!(
                            "Spark `aes_encrypt`: Mode requires a single value, got {array:?}"
                        );
                    }
                    match array.data_type() {
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Mode to StringArray"))?;
                            encryption_name_to_mode(array.value(0))
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Mode to LargeStringArray"))?;
                            encryption_name_to_mode(array.value(0))
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Mode to StringViewArray"))?;
                            encryption_name_to_mode(array.value(0))
                        },
                        other => exec_err!("Spark `aes_encrypt`: Mode array must be STRING, got array of type {other}")
                    }
                }
                other => exec_err!("Spark `aes_encrypt`: Mode must be a STRING, got {other:?}"),
            }
        } else {
            Ok(EncryptionMode::GCM)
        }?;

        let iv: Option<Vec<u8>> = if args.len() >= 5 {
            let iv = match &args[4] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(iv)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(iv)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(iv))) => Ok(iv.as_bytes()),
                ColumnarValue::Scalar(ScalarValue::Binary(Some(iv)))
                | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(iv)))
                | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(iv)))
                | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(iv))) => Ok(iv.as_slice()),
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return exec_err!(
                            "Spark `aes_encrypt`: Key requires a single value, got {array:?}"
                        );
                    }
                    match array.data_type() {
                        DataType::Binary => {
                            let array = array.as_any().downcast_ref::<BinaryArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast IV to BinaryArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::BinaryView => {
                            let array = array.as_any().downcast_ref::<BinaryViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast IV to LargeBinaryArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::FixedSizeBinary(_) => {
                            let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast IV to FixedSizeBinaryArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::LargeBinary => {
                            let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast IV to LargeBinaryArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to StringArray"))?;
                            Ok(array.value(0).as_bytes())
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to LargeStringArray"))?;
                            Ok(array.value(0).as_bytes())
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast Key to StringViewArray"))?;
                            Ok(array.value(0).as_bytes())
                        },
                        other => exec_err!("Spark `aes_encrypt`: IV must be BINARY or STRING, got array of type {other}")
                    }
                }
                other => {
                    exec_err!("Spark `aes_encrypt`: IV must be BINARY or STRING, got {other:?}")
                }
            }?;
            match &mode {
                EncryptionMode::GCM => {
                    if iv.is_empty() {
                        // If none is provided, Spark passes up an empty string.
                        Ok(Some(generate_iv(&mode)))
                    } else if iv.len() != 12 {
                        exec_err!(
                            "Spark `aes_encrypt`: IV must be 12 bytes long for GCM mode, got {}",
                            iv.len()
                        )
                    } else {
                        Ok(Some(iv.to_vec()))
                    }
                }
                EncryptionMode::CBC => {
                    if iv.is_empty() {
                        // If none is provided, Spark passes up an empty string.
                        Ok(Some(generate_iv(&mode)))
                    } else if iv.len() != 16 {
                        exec_err!(
                            "Spark `aes_encrypt`: IV must be 16 bytes long for CBC mode, got {}",
                            iv.len()
                        )
                    } else {
                        Ok(Some(iv.to_vec()))
                    }
                }
            }
        } else {
            Ok(Some(generate_iv(&mode)))
        }?;

        let aad = if args.len() >= 6 {
            match &args[5] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(aad))) => {
                    if aad.is_empty() {
                        // If none is provided, Spark passes up an empty string.
                        Ok(None)
                    } else {
                        Ok(Some(aad.as_bytes()))
                    }
                }
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return exec_err!(
                            "Spark `aes_encrypt`: AAD requires a single value, got {array:?}"
                        );
                    }
                    let aad = match array.data_type() {
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast AAD to StringArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast AAD to LargeStringArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast AAD to StringViewArray"))?;
                            Ok(array.value(0))
                        },
                        other => exec_err!("Spark `aes_encrypt`: AAD array must be STRING, got array of type {other}")
                    }?;
                    if aad.is_empty() {
                        // If none is provided, Spark passes up an empty string.
                        Ok(None)
                    } else {
                        Ok(Some(aad.as_bytes()))
                    }
                }
                other => exec_err!("Spark `aes_encrypt`: AAD must be STRING, got {other:?}"),
            }
        } else {
            Ok(None)
        }?;

        if aad.is_some() {
            match &mode {
                EncryptionMode::GCM => {}
                _ => return exec_err!("Spark `aes_decrypt`: AAD is only supported for GCM mode"),
            };
        }

        let ciphertext = match &mode {
            EncryptionMode::GCM => {
                let iv = iv.as_ref().ok_or_else(|| {
                    exec_datafusion_err!("Spark `aes_encrypt`: IV must be provided for GCM mode")
                })?;
                let nonce = Nonce::from_slice(iv);
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
                .map_err(|e| {
                    exec_datafusion_err!("Spark `aes_encrypt`: GCM Encryption error: {e}")
                })?;
                let mut ciphertext = iv.to_vec();
                ciphertext.extend_from_slice(&result);
                Ok::<Vec<u8>, DataFusionError>(ciphertext)
            }
            EncryptionMode::CBC => {
                let iv = iv.as_ref().ok_or_else(|| {
                    exec_datafusion_err!("Spark `aes_encrypt`: IV must be provided for CBC mode")
                })?;
                let result = match key.len() {
                    16 => cbc::Encryptor::<Aes128>::new_from_slices(key, iv)
                        .map_err(|e| {
                            exec_datafusion_err!(
                                "Spark `aes_encrypt`: Error creating AES-128 cipher: {e}"
                            )
                        })
                        .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                    24 => cbc::Encryptor::<Aes192>::new_from_slices(key, iv)
                        .map_err(|e| {
                            exec_datafusion_err!(
                                "Spark `aes_encrypt`: Error creating AES-192 cipher: {e}"
                            )
                        })
                        .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                    32 => cbc::Encryptor::<Aes256>::new_from_slices(key, iv)
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

        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(ciphertext))))
    }
}

#[derive(Debug)]
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

// TODO: Support array batch
impl ScalarUDFImpl for SparkAESDecrypt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_aes_decrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() < 2 || arg_types.len() > 5 {
            return exec_err!(
                "Spark `aes_decrypt` function requires 2 to 5 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Binary)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _number_rows: usize) -> Result<ColumnarValue> {
        if args.len() < 2 || args.len() > 5 {
            return exec_err!(
                "Spark `aes_decrypt` function requires 2 to 5 arguments, got {}",
                args.len()
            );
        }

        let expr = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(expr))) => Ok(expr.as_slice()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => Ok(expr.as_bytes()),
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return exec_err!(
                        "Spark `aes_decrypt`: Expr requires a single value, got {array:?}"
                    );
                }
                match array.data_type() {
                    DataType::Binary => {
                        let array = array.as_any().downcast_ref::<BinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to BinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::BinaryView => {
                        let array = array.as_any().downcast_ref::<BinaryViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::FixedSizeBinary(_) => {
                        let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to FixedSizeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::LargeBinary => {
                        let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::Utf8 => {
                        let array = array.as_any().downcast_ref::<StringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to StringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::LargeUtf8 => {
                        let array = array.as_any().downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to LargeStringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::Utf8View => {
                        let array = array.as_any().downcast_ref::<StringViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Expr to StringViewArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    other => exec_err!("Spark `aes_decrypt`: Expr array must be BINARY or STRING, got array of type {other}")
                }
            }
            other => exec_err!("Spark `aes_decrypt`: Expr must be BINARY or STRING, got {other:?}"),
        }?;

        let key = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(key)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(key))) => Ok(key.as_slice()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(key))) => Ok(key.as_bytes()),
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return exec_err!(
                        "Spark `aes_decrypt`: Key requires a single value, got {array:?}"
                    );
                }
                match array.data_type() {
                    DataType::Binary => {
                        let array = array.as_any().downcast_ref::<BinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to BinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::BinaryView => {
                        let array = array.as_any().downcast_ref::<BinaryViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::FixedSizeBinary(_) => {
                        let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to FixedSizeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::LargeBinary => {
                        let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to LargeBinaryArray"))?;
                        Ok(array.value(0))
                    },
                    DataType::Utf8 => {
                        let array = array.as_any().downcast_ref::<StringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to StringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::LargeUtf8 => {
                        let array = array.as_any().downcast_ref::<LargeStringArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to LargeStringArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    DataType::Utf8View => {
                        let array = array.as_any().downcast_ref::<StringViewArray>()
                            .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Key to StringViewArray"))?;
                        Ok(array.value(0).as_bytes())
                    },
                    other => exec_err!("Spark `aes_decrypt`: Key array must be BINARY or STRING, got array of type {other}")
                }
            }
            other => exec_err!("Spark `aes_decrypt`: Key must be BINARY, got {other:?}"),
        }?;

        let mode = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(mode)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(mode)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(mode))) => {
                    encryption_name_to_mode(mode)
                }
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return exec_err!(
                            "Spark `aes_decrypt`: Mode requires a single value, got {array:?}"
                        );
                    }
                    match array.data_type() {
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Mode to StringArray"))?;
                            encryption_name_to_mode(array.value(0))
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Mode to LargeStringArray"))?;
                            encryption_name_to_mode(array.value(0))
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast Mode to StringViewArray"))?;
                            encryption_name_to_mode(array.value(0))
                        },
                        other => exec_err!("Spark `aes_decrypt`: Mode array must be STRING, got array of type {other}")
                    }
                }
                other => exec_err!("Spark `aes_decrypt`: Mode must be a STRING, got {other:?}"),
            }
        } else {
            Ok(EncryptionMode::GCM)
        }?;

        let aad = if args.len() >= 5 {
            match &args[4] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(aad))) => {
                    if aad.is_empty() {
                        // If none is provided, Spark passes up an empty string.
                        Ok(None)
                    } else {
                        Ok(Some(aad.as_bytes()))
                    }
                }
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return exec_err!(
                            "Spark `aes_decrypt`: AAD requires a single value, got {array:?}"
                        );
                    }
                    let aad = match array.data_type() {
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast AAD to StringArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast AAD to LargeStringArray"))?;
                            Ok(array.value(0))
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_decrypt`: Failed to downcast AAD to StringViewArray"))?;
                            Ok(array.value(0))
                        },
                        other => exec_err!("Spark `aes_decrypt`: AAD array must be STRING, got array of type {other}")
                    }?;
                    if aad.is_empty() {
                        // If none is provided, Spark passes up an empty string.
                        Ok(None)
                    } else {
                        Ok(Some(aad.as_bytes()))
                    }
                }
                other => exec_err!("Spark `aes_decrypt`: AAD must be STRING, got {other:?}"),
            }
        } else {
            Ok(None)
        }?;

        if aad.is_some() {
            match &mode {
                EncryptionMode::GCM => {}
                _ => return exec_err!("Spark `aes_decrypt`: AAD is only supported for GCM mode"),
            };
        }

        let result = match &mode {
            EncryptionMode::GCM => {
                // iv is prepended to the ciphertext
                let iv = &expr[..12];
                let expr = &expr[12..];
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
                            exec_datafusion_err!("Spark `aes_decrypt`: GCM Encryption error: {e}")
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
                            exec_datafusion_err!("Spark `aes_decrypt`: GCM Encryption error: {e}")
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
                            exec_datafusion_err!("Spark `aes_decrypt`: GCM Encryption error: {e}")
                        })?;
                        Ok(result)
                    }
                    other => exec_err!(
                        "Spark `aes_decrypt`: Key length must be 16, 24, or 32 bytes, got {other}"
                    ),
                }
                .map_err(|e| {
                    exec_datafusion_err!("Spark `aes_decrypt`: GCM Encryption error: {e}")
                })?;
                Ok::<Vec<u8>, DataFusionError>(decrypted)
            }
            EncryptionMode::CBC => {
                // iv is prepended to the ciphertext
                let iv = &expr[..16];
                let expr = &expr[16..];
                let decrypted = match key.len() {
                    16 => {
                        let decryptor = cbc::Decryptor::<Aes128>::new_from_slices(key, iv)
                            .map_err(|e| {
                                exec_datafusion_err!(
                                    "Spark `aes_decrypt`: Error creating AES-128 cipher: {e}"
                                )
                            })?;
                        let result =
                            decryptor
                                .decrypt_padded_vec_mut::<Pkcs7>(expr)
                                .map_err(|e| {
                                    exec_datafusion_err!(
                                        "Spark `aes_decrypt`: CBC Decryption error: {e}"
                                    )
                                })?;
                        Ok(result)
                    }
                    24 => {
                        let decryptor = cbc::Decryptor::<Aes192>::new_from_slices(key, iv)
                            .map_err(|e| {
                                exec_datafusion_err!(
                                    "Spark `aes_decrypt`: Error creating AES-192 cipher: {e}"
                                )
                            })?;
                        let result =
                            decryptor
                                .decrypt_padded_vec_mut::<Pkcs7>(expr)
                                .map_err(|e| {
                                    exec_datafusion_err!(
                                        "Spark `aes_decrypt`: CBC Decryption error: {e}"
                                    )
                                })?;
                        Ok(result)
                    }
                    32 => {
                        let decryptor = cbc::Decryptor::<Aes256>::new_from_slices(key, iv)
                            .map_err(|e| {
                                exec_datafusion_err!(
                                    "Spark `aes_decrypt`: Error creating AES-256 cipher: {e}"
                                )
                            })?;
                        let result =
                            decryptor
                                .decrypt_padded_vec_mut::<Pkcs7>(expr)
                                .map_err(|e| {
                                    exec_datafusion_err!(
                                        "Spark `aes_decrypt`: CBC Decryption error: {e}"
                                    )
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

        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(result))))
    }
}

#[derive(Debug)]
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
            signature: Signature::variadic_any(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SparkTryAESEncrypt {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_aes_encrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let result = SparkAESEncrypt::new().invoke_batch(args, number_rows);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        }
    }
}

#[derive(Debug)]
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_aes_decrypt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        let result = SparkAESDecrypt::new().invoke_batch(args, number_rows);
        match result {
            Ok(result) => Ok(result),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Binary(None))),
        }
    }
}
