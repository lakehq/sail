use std::any::Any;
use std::fmt;

use aes::cipher::block_padding::Pkcs7;
use aes::cipher::{BlockEncryptMut, KeyIvInit};
use aes::{Aes128, Aes192, Aes256};
use aes_gcm_siv::aead::rand_core::{OsRng, RngCore};
use aes_gcm_siv::aead::{Aead, KeyInit, Payload};
use aes_gcm_siv::{Aes128GcmSiv, Aes256GcmSiv, AesGcmSiv, Nonce};
use datafusion::arrow::array::{
    BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
    StringArray, StringViewArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

pub type Aes192GcmSiv = AesGcmSiv<Aes192>;

pub fn encryption_name_to_mode(mode: &str) -> Result<EncryptionMode> {
    match mode.trim().to_uppercase().as_str() {
        "GCM" => Ok(EncryptionMode::GCM),
        "CBC" => Ok(EncryptionMode::CBC),
        "ECB" => Ok(EncryptionMode::ECB),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "Invalid encryption mode, must be one of 'GCM', 'CBC', or 'ECB'. Got {other}"
        ))),
    }
}

#[allow(clippy::upper_case_acronyms)]
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

/// Arguments
///   `expr`: The BINARY expression to be encrypted.
///   `key`: A BINARY expression. The key to be used to encrypt expr. It must be 16, 24, or 32 bytes long.
///        The algorithm depends on the length of the `key`:
///          - 16 bytes: AES-128
///          - 24 bytes: AES-192
///          - 32 bytes: AES-256
///   `mode`: An optional STRING expression describing the encryption mode.
///           `mode` must be one of (case-insensitive):
///             - 'GCM': Use Galois/Counter Mode (GCM). This is the default.
///             - 'CBC': Use Cipher-Block Chaining (CBC) mode.
///             - 'ECB': Use Electronic CodeBook (ECB) mode.
///   `padding`: An optional STRING expression describing how encryption handles padding of the value to key length.
///              `padding` must be one of (case-insensitive):
///                - 'NONE': Uses no padding. Valid only for 'GCM'.
///                - 'DEFAULT': Uses 'NONE' for 'GCM' and 'PKCS' for 'ECB', and 'CBC' mode.
///                - 'PKCS': Uses Public Key Cryptography Standards (PKCS) padding. Valid only for 'ECB' and 'CBC'.
///                          PKCS padding adds between 1 and key-length number of bytes to pad expr to a multiple of key length.
///                          The value of each pad byte is the number of bytes being padded.
///   `iv`: An optional STRING expression providing an initialization vector (IV) for GCM or CBC modes.
///         `iv`, when specified, must be 12-bytes long for GCM and 16 bytes for CBC.
///          If not provided, a random vector will be generated and prepended to the output.
///   `aad`: An optional STRING expression providing authenticated additional data (AAD) in GCM mode.
///          Optional additional authenticated data (AAD) is only supported for GCM.
///          If provided for encryption, the identical AAD value must be provided for decryption.

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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
            other => exec_err!("Key must be BINARY, got {other:?}"),
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
                other => exec_err!("Mode must be a STRING, got {other:?}"),
            }
        } else {
            Ok(EncryptionMode::GCM)
        }?;

        let iv: Option<Vec<u8>> = if args.len() >= 5 {
            let iv = match &args[4] {
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
                            other => exec_err!("Spark `aes_encrypt`: IV must be BINARY or STRING, got array of type {other}")
                    }
                }
                other => exec_err!("IV must be BINARY, got {other:?}"),
            }?;
            match &mode {
                EncryptionMode::GCM => {
                    if iv.len() != 12 {
                        exec_err!("IV must be 12 bytes long for GCM mode, got {}", iv.len())
                    } else {
                        Ok(Some(iv.to_vec()))
                    }
                }
                EncryptionMode::CBC => {
                    if iv.len() != 16 {
                        exec_err!("IV must be 16 bytes long for CBC mode, got {}", iv.len())
                    } else {
                        Ok(Some(iv.to_vec()))
                    }
                }
                EncryptionMode::ECB => exec_err!("IV not supported for ECB mode"),
            }
        } else {
            match &mode {
                EncryptionMode::GCM => {
                    let mut iv = [0u8; 12];
                    OsRng.fill_bytes(&mut iv);
                    Ok(Some(iv.to_vec()))
                }
                EncryptionMode::CBC => {
                    let mut iv = [0u8; 16];
                    OsRng.fill_bytes(&mut iv);
                    Ok(Some(iv.to_vec()))
                }
                EncryptionMode::ECB => Ok(None),
            }
        }?;

        let aad = if args.len() >= 6 {
            match &mode {
                EncryptionMode::GCM => {}
                _ => return exec_err!("AAD is only supported for GCM mode"),
            };
            match &args[5] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(aad))) => {
                    Ok(Some(aad.as_bytes()))
                }
                ColumnarValue::Array(array) => {
                    if array.len() != 1 {
                        return exec_err!(
                            "Spark `aes_encrypt`: AAD requires a single value, got {array:?}"
                        );
                    }
                    match array.data_type() {
                        DataType::Utf8 => {
                            let array = array.as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast AAD to StringArray"))?;
                            Ok(Some(array.value(0).as_bytes()))
                        },
                        DataType::LargeUtf8 => {
                            let array = array.as_any().downcast_ref::<LargeStringArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast AAD to LargeStringArray"))?;
                            Ok(Some(array.value(0).as_bytes()))
                        },
                        DataType::Utf8View => {
                            let array = array.as_any().downcast_ref::<StringViewArray>()
                                .ok_or_else(|| exec_datafusion_err!("Spark `aes_encrypt`: Failed to downcast AAD to StringViewArray"))?;
                            Ok(Some(array.value(0).as_bytes()))
                        },
                        other => exec_err!("Spark `aes_encrypt`: AAD array must be STRING, got array of type {other}")
                    }
                }
                other => exec_err!("AAD must be STRING, got {other:?}"),
            }
        } else {
            Ok(None)
        }?;

        let ciphertext = match &mode {
            EncryptionMode::GCM => {
                let iv = iv
                    .as_ref()
                    .ok_or_else(|| exec_datafusion_err!("IV must be provided for GCM mode"))?;
                let nonce = Nonce::from_slice(iv);
                let result = match key.len() {
                    16 => {
                        let cipher = Aes128GcmSiv::new_from_slice(key).map_err(|e| {
                            exec_datafusion_err!("Error creating AES-128 cipher: {e}")
                        })?;
                        let result = match aad {
                            Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                            None => cipher.encrypt(nonce, expr),
                        }
                        .map_err(|e| exec_datafusion_err!("GCM Encryption error: {e}"))?;
                        Ok(result)
                    }
                    24 => {
                        let cipher = Aes192GcmSiv::new_from_slice(key).map_err(|e| {
                            exec_datafusion_err!("Error creating AES-192 cipher: {e}")
                        })?;
                        let result = match aad {
                            Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                            None => cipher.encrypt(nonce, expr),
                        }
                        .map_err(|e| exec_datafusion_err!("GCM Encryption error: {e}"))?;
                        Ok(result)
                    }
                    32 => {
                        let cipher = Aes256GcmSiv::new_from_slice(key).map_err(|e| {
                            exec_datafusion_err!("Error creating AES-256 cipher: {e}")
                        })?;
                        let result = match aad {
                            Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                            None => cipher.encrypt(nonce, expr),
                        }
                        .map_err(|e| exec_datafusion_err!("GCM Encryption error: {e}"))?;
                        Ok(result)
                    }
                    other => exec_err!("Key length must be 16, 24, or 32 bytes, got {other}"),
                }
                .map_err(|e| exec_datafusion_err!("GCM Encryption error: {e}"))?;
                let mut ciphertext = iv.to_vec();
                ciphertext.extend_from_slice(&result);
                Ok(ciphertext)
            }
            EncryptionMode::CBC => {
                let iv = iv
                    .as_ref()
                    .ok_or_else(|| exec_datafusion_err!("IV must be provided for CBC mode"))?;
                let result = match key.len() {
                    16 => cbc::Encryptor::<Aes128>::new_from_slices(key, iv)
                        .map_err(|e| exec_datafusion_err!("Error creating AES-128 cipher: {e}"))
                        .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                    24 => cbc::Encryptor::<Aes192>::new_from_slices(key, iv)
                        .map_err(|e| exec_datafusion_err!("Error creating AES-192 cipher: {e}"))
                        .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                    32 => cbc::Encryptor::<Aes256>::new_from_slices(key, iv)
                        .map_err(|e| exec_datafusion_err!("Error creating AES-256 cipher: {e}"))
                        .map(|enc| enc.encrypt_padded_vec_mut::<Pkcs7>(expr)),
                    other => exec_err!("Key length must be 16, 24, or 32 bytes, got {other}"),
                }?;
                let mut ciphertext = iv.to_vec();
                ciphertext.extend_from_slice(&result);
                Ok(ciphertext)
            }
            EncryptionMode::ECB => exec_err!("ECB mode not implemented for aes_encrypt"),
        }?;

        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(ciphertext))))
    }
}
