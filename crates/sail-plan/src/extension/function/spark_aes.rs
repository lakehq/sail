use std::any::Any;

use aes_gcm_siv::aead::rand_core::{OsRng, RngCore};
use aes_gcm_siv::aead::{Aead, KeyInit, Payload};
use aes_gcm_siv::{Aes128GcmSiv, Aes256GcmSiv, AesGcmSiv, Nonce};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

pub type Aes192GcmSiv = AesGcmSiv<aes::Aes192>;

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
///   `aad`: An optional STRING expression providing authenticated additional data (AAD) in GCM mode.

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
            signature: Signature::variadic_any(Volatility::Immutable),
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
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(expr))) => expr,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(expr)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(expr))) => expr.as_bytes(),
            _ => return exec_err!("First argument (expr) must be BINARY"),
        };

        let key = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(_, Some(key)))
            | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(key))) => key,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(key)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(key))) => key.as_bytes(),
            _ => return exec_err!("Second argument (key) must be BINARY"),
        };

        let mode = if args.len() >= 3 {
            match &args[2] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(mode)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(mode)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(mode))) => {
                    match mode.to_uppercase().as_str() {
                        "GCM" | "" => "GCM",
                        "CBC" => return exec_err!("CBC mode not implemented"),
                        "ECB" => return exec_err!("ECB mode not implemented"),
                        other => return exec_err!("Invalid encryption mode: {other}"),
                    }
                }
                other => return exec_err!("Mode must be a STRING, got {other:?}"),
            }
        } else {
            "GCM"
        };

        let _padding = if args.len() >= 4 {
            match &args[3] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(padding)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(padding)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(padding))) => {
                    match padding.to_string().to_uppercase().as_str() {
                        "NONE" | "" => "NONE",
                        "DEFAULT" => match mode {
                            "GCM" => "NONE",
                            "CBC" | "ECB" => "PKCS",
                            _ => return exec_err!("Invalid encryption mode: {mode}"),
                        },
                        "PKCS" => match mode {
                            "GCM" => return exec_err!("PKCS padding not supported for GCM mode"),
                            "CBC" | "ECB" => "PKCS",
                            _ => return exec_err!("Invalid encryption mode: {mode}"),
                        },
                        other => return exec_err!("Invalid padding mode: {other}"),
                    }
                }
                other => return exec_err!("Padding must be a STRING, got {other:?}"),
            }
        } else {
            "NONE"
        };

        let iv: Vec<u8> = if args.len() >= 5 {
            match &args[4] {
                ColumnarValue::Scalar(ScalarValue::Binary(Some(iv))) => {
                    match mode {
                        "GCM" => {
                            if iv.len() != 12 {
                                return exec_err!("IV must be 12 bytes long for GCM mode");
                            }
                        }
                        "CBC" => {
                            if iv.len() != 16 {
                                return exec_err!("IV must be 16 bytes long for CBC mode");
                            }
                        }
                        other => return exec_err!("IV not supported for {other} mode"),
                    }
                    iv.to_vec()
                }
                other => return exec_err!("IV must be BINARY, got {other:?}"),
            }
        } else {
            match mode {
                "GCM" => {
                    let mut iv = [0u8; 12];
                    OsRng.fill_bytes(&mut iv);
                    iv.to_vec()
                }
                "CBC" => {
                    return exec_err!("IV not supported for CBC mode");
                    // let mut iv = [0u8; 16];
                    // OsRng.fill_bytes(&mut iv);
                    // iv.to_vec()
                }
                other => return exec_err!("IV not supported for {other} mode"),
            }
        };

        // TODO: Nonce is GenericArray<u8, U12>;
        //  So once CBC mode is implemented, we need to handle this differently
        let nonce = Nonce::from_slice(&iv);

        let aad = if args.len() >= 6 {
            match &args[5] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(aad)))
                | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(aad))) => Some(aad.as_bytes()),
                other => return exec_err!("AAD must be STRING, got {other:?}"),
            }
        } else {
            None
        };

        let ciphertext = match key.len() {
            16 => {
                let cipher = Aes128GcmSiv::new_from_slice(key)
                    .map_err(|e| exec_datafusion_err!("Error creating AES-128 cipher: {e}"))?;
                match aad {
                    Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                    None => cipher.encrypt(nonce, expr),
                }
            }
            24 => {
                let cipher = Aes192GcmSiv::new_from_slice(key)
                    .map_err(|e| exec_datafusion_err!("Error creating AES-192 cipher: {e}"))?;
                match aad {
                    Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                    None => cipher.encrypt(nonce, expr),
                }
            }
            32 => {
                let cipher = Aes256GcmSiv::new_from_slice(key)
                    .map_err(|e| exec_datafusion_err!("Error creating AES-256 cipher: {e}"))?;
                match aad {
                    Some(aad) => cipher.encrypt(nonce, Payload { msg: expr, aad }),
                    None => cipher.encrypt(nonce, expr),
                }
            }
            other => {
                return exec_err!("Key length must be 16, 24, or 32 bytes, got {other}");
            }
        }
        .map_err(|e| exec_datafusion_err!("Encryption error: {e}"))?;

        // Combine nonce and ciphertext for output. The same nonce is used for decryption.
        let mut result = iv.to_vec();
        result.extend_from_slice(&ciphertext);
        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(result))))
    }
}
