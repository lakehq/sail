/// User agent for Sail requests to remote services.
///
/// References:
///   * <https://www.rfc-editor.org/info/rfc9110/#name-user-agent>
///   * <https://www.rfc-editor.org/info/rfc7231/#section-5.5.3> (obsolete)
///   * <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/User-Agent>
pub const SAIL_USER_AGENT: &str = concat!("Sail/", env!("CARGO_PKG_VERSION"));
