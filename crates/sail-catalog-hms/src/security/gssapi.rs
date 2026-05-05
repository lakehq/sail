use std::ffi::OsStr;
use std::marker::PhantomData;
use std::ops::Deref;
use std::os::raw::c_void;
use std::sync::{Arc, LazyLock, Mutex};
use std::{ptr, slice};

use sail_catalog::error::{CatalogError, CatalogResult};

use super::thrift_sasl::SaslClientSession;

mod bindings {
    #![allow(
        non_camel_case_types,
        non_snake_case,
        non_upper_case_globals,
        clippy::expect_used,
        clippy::panic,
        clippy::unreadable_literal,
        clippy::missing_safety_doc,
        warnings
    )]
    include!("./gssapi_bindings.rs");

    // SAFETY: Function pointers from libloading::Library are stateless (pure FFI calls).
    // gss_OID globals are immutable once loaded through `LazyLock<Option<GSSAPI>>`.
    unsafe impl Send for GSSAPI {}
    unsafe impl Sync for GSSAPI {}
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug)]
    struct GssMajorCodes: u32 {
        const GSS_S_CALL_INACCESSIBLE_READ = bindings::_GSS_S_CALL_INACCESSIBLE_READ;
        const GSS_S_CALL_INACCESSIBLE_WRITE = bindings::_GSS_S_CALL_INACCESSIBLE_WRITE;
        const GSS_S_CALL_BAD_STRUCTURE = bindings::_GSS_S_CALL_BAD_STRUCTURE;
        const GSS_S_BAD_MECH = bindings::_GSS_S_BAD_MECH;
        const GSS_S_BAD_NAME = bindings::_GSS_S_BAD_NAME;
        const GSS_S_BAD_NAMETYPE = bindings::_GSS_S_BAD_NAMETYPE;
        const GSS_S_BAD_BINDINGS = bindings::_GSS_S_BAD_BINDINGS;
        const GSS_S_BAD_STATUS = bindings::_GSS_S_BAD_STATUS;
        const GSS_S_BAD_SIG = bindings::_GSS_S_BAD_SIG;
        const GSS_S_BAD_MIC = bindings::_GSS_S_BAD_MIC;
        const GSS_S_NO_CRED = bindings::_GSS_S_NO_CRED;
        const GSS_S_NO_CONTEXT = bindings::_GSS_S_NO_CONTEXT;
        const GSS_S_DEFECTIVE_TOKEN = bindings::_GSS_S_DEFECTIVE_TOKEN;
        const GSS_S_DEFECTIVE_CREDENTIAL = bindings::_GSS_S_DEFECTIVE_CREDENTIAL;
        const GSS_S_CREDENTIALS_EXPIRED = bindings::_GSS_S_CREDENTIALS_EXPIRED;
        const GSS_S_CONTEXT_EXPIRED = bindings::_GSS_S_CONTEXT_EXPIRED;
        const GSS_S_FAILURE = bindings::_GSS_S_FAILURE;
        const GSS_S_BAD_QOP = bindings::_GSS_S_BAD_QOP;
        const GSS_S_UNAUTHORIZED = bindings::_GSS_S_UNAUTHORIZED;
        const GSS_S_UNAVAILABLE = bindings::_GSS_S_UNAVAILABLE;
        const GSS_S_DUPLICATE_ELEMENT = bindings::_GSS_S_DUPLICATE_ELEMENT;
        const GSS_S_NAME_NOT_MN = bindings::_GSS_S_NAME_NOT_MN;
        const GSS_S_CONTINUE_NEEDED = bindings::_GSS_S_CONTINUE_NEEDED;
        const GSS_S_DUPLICATE_TOKEN = bindings::_GSS_S_DUPLICATE_TOKEN;
        const GSS_S_OLD_TOKEN = bindings::_GSS_S_OLD_TOKEN;
        const GSS_S_UNSEQ_TOKEN = bindings::_GSS_S_UNSEQ_TOKEN;
        const GSS_S_GAP_TOKEN = bindings::_GSS_S_GAP_TOKEN;
    }
}

static LIBGSSAPI: LazyLock<Option<bindings::GSSAPI>> =
    LazyLock::new(|| load_gssapi_library_impl(default_gssapi_library_name()).ok());

#[derive(Debug)]
pub(crate) struct GssapiSession {
    state: GssapiState,
    qop_min: SaslQop,
}

#[derive(Debug)]
enum GssapiState {
    Pending(GssClientContext),
    Last(GssClientContext),
    Complete(Option<GssapiFrameProtector>),
    Errored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SaslQop {
    Auth,
    AuthInt,
    AuthConf,
}

impl SaslQop {
    pub(crate) fn from_config(value: &str) -> CatalogResult<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auth" => Ok(Self::Auth),
            "auth_int" | "auth-int" => Ok(Self::AuthInt),
            "auth_conf" | "auth-conf" => Ok(Self::AuthConf),
            other => Err(CatalogError::InvalidArgument(format!(
                "Unsupported HMS min_sasl_qop '{other}', expected 'auth', 'auth_int', or 'auth_conf'"
            ))),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecurityLayer {
    None = 1,
    Integrity = 2,
    Confidentiality = 4,
}

#[derive(Debug)]
struct SecurityLayerNegotiation {
    selected_layer: SecurityLayer,
    response: [u8; 4],
}

#[derive(Debug)]
struct GssapiFrameProtectorInner {
    context: GssClientContext,
    encrypt: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct GssapiFrameProtector {
    inner: Arc<Mutex<GssapiFrameProtectorInner>>,
}

impl GssapiFrameProtector {
    fn new(context: GssClientContext, layer: SecurityLayer) -> Self {
        Self {
            inner: Arc::new(Mutex::new(GssapiFrameProtectorInner {
                context,
                encrypt: matches!(layer, SecurityLayer::Confidentiality),
            })),
        }
    }

    pub(crate) fn wrap(&self, payload: &[u8]) -> CatalogResult<Vec<u8>> {
        let mut inner = self.inner.lock().map_err(|_| {
            CatalogError::External("Kerberos SASL frame wrapper lock poisoned".to_string())
        })?;
        let encrypt = inner.encrypt;
        inner.context.wrap(encrypt, payload)
    }

    pub(crate) fn unwrap(&self, payload: &[u8]) -> CatalogResult<Vec<u8>> {
        let mut inner = self.inner.lock().map_err(|_| {
            CatalogError::External("Kerberos SASL frame wrapper lock poisoned".to_string())
        })?;
        inner.context.unwrap(payload)
    }
}

#[cfg(test)]
pub(crate) fn load_gssapi_library_by_name_for_test(name: impl AsRef<OsStr>) -> CatalogResult<()> {
    load_gssapi_library_impl(name).map(|_| ())
}

fn default_gssapi_library_name() -> std::ffi::OsString {
    #[cfg(target_os = "linux")]
    {
        "libgssapi_krb5.so.2".into()
    }

    #[cfg(not(target_os = "linux"))]
    {
        libloading::library_filename("gssapi_krb5")
    }
}

fn load_gssapi_library_impl(name: impl AsRef<OsStr>) -> CatalogResult<bindings::GSSAPI> {
    unsafe { bindings::GSSAPI::new(name.as_ref()) }.map_err(|error| {
        #[cfg(target_os = "macos")]
        let message = "Try installing Kerberos runtime libraries via \"brew install krb5\"";
        #[cfg(target_os = "linux")]
        let message = "On Debian-based systems try \"apt-get install libgssapi-krb5-2\". On RHEL-based systems try \"yum install krb5-libs\"";
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let message = "Loading Kerberos runtime libraries is not supported on this platform";

        CatalogError::External(format!(
            "Failed to load Kerberos runtime library '{}': {error}. {message}",
            name.as_ref().to_string_lossy()
        ))
    })
}

fn libgssapi() -> CatalogResult<&'static bindings::GSSAPI> {
    LIBGSSAPI.as_ref().ok_or_else(|| {
        CatalogError::External(
            "Failed to load Kerberos runtime library. Install the GSSAPI runtime and run kinit before starting Sail."
                .to_string(),
        )
    })
}

/// Returns `true` if the GSSAPI/Kerberos runtime library was successfully loaded.
pub(crate) fn is_available() -> bool {
    LIBGSSAPI.is_some()
}

#[repr(transparent)]
struct GssBuf<'a>(bindings::gss_buffer_desc_struct, PhantomData<&'a [u8]>);

impl GssBuf<'_> {
    fn empty() -> Self {
        Self(
            bindings::gss_buffer_desc_struct {
                length: 0,
                value: ptr::null_mut(),
            },
            PhantomData,
        )
    }

    unsafe fn as_ptr(&mut self) -> bindings::gss_buffer_t {
        &mut self.0 as bindings::gss_buffer_t
    }

    fn to_vec(&self) -> Vec<u8> {
        self.deref().to_vec()
    }

    /// Releases a GSSAPI-allocated buffer by calling `gss_release_buffer`.
    /// Must be called after `to_vec()` or `deref()` when the buffer was
    /// allocated by a GSSAPI function (e.g. `gss_init_sec_context` output,
    /// `gss_wrap`/`gss_unwrap` output, or `gss_display_status` message).
    fn release(&mut self) {
        if !self.0.value.is_null() {
            let mut minor = bindings::GSS_S_COMPLETE;
            // SAFETY: Error is intentionally swallowed because:
            // (a) libgssapi() cannot fail with LazyLock, and
            // (b) the null/length reset below prevents double-free regardless.
            let _ =
                unsafe { libgssapi().map(|gss| gss.gss_release_buffer(&mut minor, &mut self.0)) };
            self.0.value = ptr::null_mut();
            self.0.length = 0;
        }
    }
}

impl Deref for GssBuf<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        if self.0.value.is_null() && self.0.length == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(self.0.value.cast(), self.0.length) }
        }
    }
}

impl<'a> From<&'a [u8]> for GssBuf<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(
            bindings::gss_buffer_desc_struct {
                length: value.len(),
                value: value.as_ptr() as *mut c_void,
            },
            PhantomData,
        )
    }
}

impl<'a> From<&'a str> for GssBuf<'a> {
    fn from(value: &'a str) -> Self {
        Self(
            bindings::gss_buffer_desc_struct {
                length: value.len(),
                value: value.as_ptr() as *mut c_void,
            },
            PhantomData,
        )
    }
}

#[derive(Debug)]
struct GssName {
    name: bindings::gss_name_t,
}

impl GssName {
    fn with_target(target: &str) -> CatalogResult<Self> {
        let mut minor = bindings::GSS_S_COMPLETE;
        let mut name = ptr::null_mut::<bindings::gss_name_struct>();
        let mut name_buf = GssBuf::from(target);

        let major = unsafe {
            libgssapi()?.gss_import_name(
                &mut minor,
                name_buf.as_ptr(),
                *libgssapi()?.GSS_C_NT_HOSTBASED_SERVICE(),
                &mut name as *mut bindings::gss_name_t,
            )
        };
        check_gss_ok(major, minor)?;
        Ok(Self { name })
    }
}

impl Drop for GssName {
    fn drop(&mut self) {
        if !self.name.is_null() {
            let mut minor = bindings::GSS_S_COMPLETE;
            let _ =
                unsafe { libgssapi().map(|gss| gss.gss_release_name(&mut minor, &mut self.name)) };
        }
    }
}

#[derive(Debug)]
struct GssClientContext {
    ctx: bindings::gss_ctx_id_t,
    target: GssName,
    flags: u32,
}

// SAFETY: GssClientContext is only accessed through `&mut self` methods.
// At the provider level, the context is wrapped in `Arc<Mutex<...>>` via
// `GssapiFrameProtector`, ensuring exclusive access across threads.
unsafe impl Send for GssClientContext {}
unsafe impl Sync for GssClientContext {}

impl GssClientContext {
    fn new(target: GssName) -> Self {
        let flags = bindings::GSS_C_DELEG_FLAG
            | bindings::GSS_C_MUTUAL_FLAG
            | bindings::GSS_C_REPLAY_FLAG
            | bindings::GSS_C_SEQUENCE_FLAG
            | bindings::GSS_C_CONF_FLAG
            | bindings::GSS_C_INTEG_FLAG
            | bindings::GSS_C_ANON_FLAG
            | bindings::GSS_C_PROT_READY_FLAG
            | bindings::GSS_C_TRANS_FLAG
            | bindings::GSS_C_DELEG_POLICY_FLAG;
        Self {
            ctx: ptr::null_mut(),
            target,
            flags,
        }
    }

    fn step(&mut self, token: Option<&[u8]>) -> CatalogResult<(Option<Vec<u8>>, bool)> {
        let mut minor = 0;
        let mut flags_out = 0;
        let mut out = bindings::gss_buffer_desc_struct {
            value: ptr::null_mut(),
            length: 0,
        };

        let mut token_buf = token.map(GssBuf::from);
        let token_ptr = token_buf
            .as_mut()
            .map(|buf| unsafe { buf.as_ptr() })
            .unwrap_or(ptr::null_mut());

        let major = unsafe {
            libgssapi()?.gss_init_sec_context(
                &mut minor,
                ptr::null_mut(),
                &mut self.ctx as *mut bindings::gss_ctx_id_t,
                self.target.name,
                *libgssapi()?.gss_mech_krb5() as bindings::gss_OID,
                self.flags,
                bindings::_GSS_C_INDEFINITE,
                ptr::null_mut(),
                token_ptr,
                ptr::null_mut(),
                &mut out,
                &mut flags_out,
                ptr::null_mut(),
            )
        };

        let complete = if major == bindings::GSS_S_CONTINUE_NEEDED {
            false
        } else {
            check_gss_ok(major, minor)?;
            true
        };

        self.flags |= flags_out;

        let out_token = unsafe {
            if out.value.is_null() {
                None
            } else {
                let vec = slice::from_raw_parts(out.value.cast(), out.length).to_vec();
                let mut out_buf = GssBuf(out, PhantomData);
                out_buf.release();
                Some(vec)
            }
        };

        Ok((out_token, complete))
    }

    fn wrap(&mut self, encrypt: bool, buf: &[u8]) -> CatalogResult<Vec<u8>> {
        let mut minor = 0;
        let mut buf_in = GssBuf::from(buf);
        let mut buf_out = GssBuf::empty();
        let major = unsafe {
            libgssapi()?.gss_wrap(
                &mut minor,
                self.ctx,
                if encrypt { 1 } else { 0 },
                bindings::GSS_C_QOP_DEFAULT,
                buf_in.as_ptr(),
                ptr::null_mut(),
                buf_out.as_ptr(),
            )
        };
        check_gss_ok(major, minor)?;
        let result = buf_out.to_vec();
        buf_out.release();
        Ok(result)
    }

    fn unwrap(&mut self, buf: &[u8]) -> CatalogResult<Vec<u8>> {
        let mut minor = 0;
        let mut buf_in = GssBuf::from(buf);
        let mut buf_out = GssBuf::empty();
        let major = unsafe {
            libgssapi()?.gss_unwrap(
                &mut minor,
                self.ctx,
                buf_in.as_ptr(),
                buf_out.as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        check_gss_ok(major, minor)?;
        let result = buf_out.to_vec();
        buf_out.release();
        Ok(result)
    }
}

impl Drop for GssClientContext {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            let mut minor = bindings::GSS_S_COMPLETE;
            let _ = unsafe {
                libgssapi().map(|gss| {
                    gss.gss_delete_sec_context(&mut minor, &mut self.ctx, ptr::null_mut())
                })
            };
        }
    }
}

fn check_gss_ok(mut major: u32, minor: u32) -> CatalogResult<()> {
    major &= (bindings::_GSS_C_CALLING_ERROR_MASK << bindings::GSS_C_CALLING_ERROR_OFFSET)
        | (bindings::_GSS_C_ROUTINE_ERROR_MASK << bindings::GSS_C_ROUTINE_ERROR_OFFSET);
    if major == bindings::GSS_S_COMPLETE {
        return Ok(());
    }

    fn display_status(code: u32, type_: i32) -> CatalogResult<String> {
        let mut context = 0;
        let mut message = GssBuf::empty();
        let mut minor = 0;
        let ret = unsafe {
            libgssapi()?.gss_display_status(
                &mut minor,
                code,
                type_,
                ptr::null_mut(),
                &mut context,
                message.as_ptr(),
            )
        };
        if ret == bindings::GSS_S_COMPLETE {
            let s = String::from_utf8_lossy(message.as_ref()).to_string();
            message.release();
            Ok(s)
        } else {
            Ok(format!("(failed to display status {code})"))
        }
    }

    let major_message = display_status(major, bindings::GSS_C_GSS_CODE as i32)?;
    let minor_message = display_status(minor, bindings::GSS_C_MECH_CODE as i32)?;

    Err(CatalogError::External(format!(
        "gssapi error: {:?} (minor {minor}): {major_message}. {minor_message}",
        GssMajorCodes::from_bits_retain(major)
    )))
}

fn select_security_layer(
    unwrapped: &[u8],
    qop_min: SaslQop,
) -> CatalogResult<SecurityLayerNegotiation> {
    if unwrapped.len() < 4 {
        return Err(CatalogError::External(
            "Invalid Kerberos SASL final token length".to_string(),
        ));
    }

    let supported = unwrapped[0];
    let supports_auth = supported & SecurityLayer::None as u8 > 0;
    let supports_auth_int = supported & SecurityLayer::Integrity as u8 > 0;
    let supports_auth_conf = supported & SecurityLayer::Confidentiality as u8 > 0;

    let selected_layer = match qop_min {
        SaslQop::AuthConf => {
            if supports_auth_conf {
                SecurityLayer::Confidentiality
            } else {
                return Err(CatalogError::NotSupported(
                    "Kerberos-secured HMS cannot satisfy `min_sasl_qop = auth_conf` because the server did not advertise auth-conf"
                        .to_string(),
                ));
            }
        }
        SaslQop::AuthInt => {
            if supports_auth_conf {
                SecurityLayer::Confidentiality
            } else if supports_auth_int {
                SecurityLayer::Integrity
            } else {
                return Err(CatalogError::NotSupported(
                    "Kerberos-secured HMS cannot satisfy `min_sasl_qop = auth_int` because the server advertised only auth"
                        .to_string(),
                ));
            }
        }
        SaslQop::Auth => {
            if supports_auth_conf {
                SecurityLayer::Confidentiality
            } else if supports_auth_int {
                SecurityLayer::Integrity
            } else if supports_auth {
                SecurityLayer::None
            } else {
                return Err(CatalogError::External(
                    "Kerberos-secured HMS did not advertise a supported SASL security layer"
                        .to_string(),
                ));
            }
        }
    };

    let server_max_buffer =
        ((unwrapped[1] as u32) << 16) | ((unwrapped[2] as u32) << 8) | unwrapped[3] as u32;
    let selected_max_buffer = server_max_buffer.min(0x00FF_FFFF);

    Ok(SecurityLayerNegotiation {
        selected_layer,
        response: [
            selected_layer as u8,
            ((selected_max_buffer >> 16) & 0xFF) as u8,
            ((selected_max_buffer >> 8) & 0xFF) as u8,
            (selected_max_buffer & 0xFF) as u8,
        ],
    })
}

impl GssapiSession {
    pub(crate) fn new(target: &str, qop_min: SaslQop) -> CatalogResult<Self> {
        let target = GssName::with_target(target)?;
        Ok(Self {
            state: GssapiState::Pending(GssClientContext::new(target)),
            qop_min,
        })
    }

    pub(crate) fn take_frame_protector(&mut self) -> CatalogResult<Option<GssapiFrameProtector>> {
        match std::mem::replace(&mut self.state, GssapiState::Errored) {
            GssapiState::Complete(frame_protector) => Ok(frame_protector),
            _ => Err(CatalogError::Internal(
                "Kerberos SASL frame protector requested before handshake completion".to_string(),
            )),
        }
    }
}

impl SaslClientSession for GssapiSession {
    fn mechanism(&self) -> &'static str {
        "GSSAPI"
    }

    fn step(&mut self, token: Option<&[u8]>) -> CatalogResult<(Vec<u8>, bool)> {
        match std::mem::replace(&mut self.state, GssapiState::Errored) {
            GssapiState::Pending(mut ctx) => {
                let (out_token, complete) = ctx.step(token)?;
                if complete {
                    self.state = GssapiState::Last(ctx);
                } else {
                    self.state = GssapiState::Pending(ctx);
                }
                Ok((out_token.unwrap_or_default(), false))
            }
            GssapiState::Last(mut ctx) => {
                let input = token.ok_or_else(|| {
                    CatalogError::External(
                        "Kerberos SASL negotiation expected a final security-layer token"
                            .to_string(),
                    )
                })?;
                let unwrapped = ctx.unwrap(input)?;
                let selection = select_security_layer(&unwrapped, self.qop_min)?;
                let wrapped = ctx.wrap(false, &selection.response)?;
                let frame_protector = if selection.selected_layer == SecurityLayer::None {
                    None
                } else {
                    Some(GssapiFrameProtector::new(ctx, selection.selected_layer))
                };
                self.state = GssapiState::Complete(frame_protector);
                Ok((wrapped, true))
            }
            GssapiState::Complete(_) | GssapiState::Errored => Err(CatalogError::External(
                "Kerberos SASL session is in an error state".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used)]

    use sail_catalog::error::CatalogError;

    use super::{
        load_gssapi_library_by_name_for_test, select_security_layer, SaslQop, SecurityLayer,
    };

    fn build_error_message(name: &str) -> String {
        load_gssapi_library_by_name_for_test(name)
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn test_missing_gssapi_runtime_library_is_reported() {
        let error = build_error_message("definitely-missing-gssapi-library");
        assert!(error.contains("Failed to load Kerberos runtime library"));
    }

    #[test]
    fn test_select_security_layer_prefers_strongest_when_auth_is_allowed() {
        let selection =
            select_security_layer(&[0b0000_0111, 0x00, 0x10, 0x00], SaslQop::Auth).unwrap();
        assert_eq!(selection.selected_layer, SecurityLayer::Confidentiality);
        assert_eq!(
            selection.response,
            [SecurityLayer::Confidentiality as u8, 0x00, 0x10, 0x00]
        );
    }

    #[test]
    fn test_auth_int_floor_rejects_auth_only_server() {
        let error = select_security_layer(
            &[SecurityLayer::None as u8, 0x00, 0x10, 0x00],
            SaslQop::AuthInt,
        )
        .unwrap_err();
        assert!(matches!(error, CatalogError::NotSupported(_)));
        assert!(error.to_string().contains("auth_int"));
    }

    #[test]
    fn test_auth_conf_floor_requires_confidentiality() {
        let error = select_security_layer(
            &[SecurityLayer::Integrity as u8, 0x00, 0x10, 0x00],
            SaslQop::AuthConf,
        )
        .unwrap_err();
        assert!(matches!(error, CatalogError::NotSupported(_)));
        assert!(error.to_string().contains("auth_conf"));
    }
}
