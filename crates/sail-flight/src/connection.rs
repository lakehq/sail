use std::collections::HashMap;
use std::sync::Arc;

use sail_catalog::manager::CatalogManager;
use sail_catalog::utils::quote_names_if_needed;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use tonic::{Request, Response, Status};

use crate::protocol::session_option_value::Value;
use crate::protocol::{
    SessionOptionError, SessionOptionValue, SessionOptions, SetSessionOptionsResult,
};
use crate::query::{internal, invalid};
use crate::service::SailFlightSqlService;
use crate::state::{FlightSession, new_handle};
use crate::transport::FlightConnectionInfo;

/// Use the cookie name suggested by Flight's experimental session protocol in
/// `SetSessionOptionsRequest`. Implementations may choose another name; Sail
/// follows this convention for sessions, including anonymous ones, independently
/// of authentication.
/// See <https://github.com/apache/arrow/blob/main/format/Flight.proto>.
const SESSION_COOKIE_NAME: &str = "arrow_flight_session_id";

impl SailFlightSqlService {
    pub(super) async fn session<T>(
        &self,
        request: &Request<T>,
        handshake: bool,
        create: bool,
    ) -> Result<Arc<FlightSession>, Status> {
        let authorization = request
            .metadata()
            .get("authorization")
            .map(|v| v.to_str())
            .transpose()
            .map_err(|_| Status::unauthenticated("invalid authorization header"))?;
        let bearer = authorization.and_then(|s| s.strip_prefix("Bearer "));
        let cookie = request
            .metadata()
            .get("cookie")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| {
                s.split(';').find_map(|part| {
                    part.trim()
                        .split_once('=')
                        .filter(|(key, _)| *key == SESSION_COOKIE_NAME)
                        .map(|(_, value)| value)
                })
            });
        let connection = request.extensions().get::<FlightConnectionInfo>();
        // Serialize initial session creation on a connection. The connection owns
        // this reference, so a reused source port cannot inherit it.
        let mut transport_session = if let Some(connection) = connection {
            Some(connection.session_id.lock().await)
        } else {
            None
        };
        let id = bearer.or(cookie).map(str::to_string).or_else(|| {
            if handshake {
                None
            } else {
                transport_session.as_deref().cloned().flatten()
            }
        });
        let (id, context) = if let Some(id) = id {
            if !self
                .state
                .lock()
                .map_err(internal)?
                .sessions
                .contains_key(&id)
            {
                return Err(Status::not_found("unknown Flight SQL session"));
            }
            let context = self
                .session_manager
                .get_session_context(id.clone())
                .await
                .map_err(internal)?
                .ok_or_else(|| Status::not_found("Flight SQL session has expired or closed"))?;
            (id, context)
        } else {
            if !create {
                return Err(Status::not_found("missing Flight SQL session"));
            }
            let id = new_handle();
            self.state
                .lock()
                .map_err(internal)?
                .sessions
                .insert(id.clone(), "anonymous".to_string());
            let context = match self
                .session_manager
                .get_or_create_session_context(id.clone(), "anonymous".to_string())
                .await
            {
                Ok(context) => context,
                Err(error) => {
                    self.state.lock().map_err(internal)?.remove_session(&id);
                    return Err(internal(error));
                }
            };
            (id, context)
        };
        if let Some(transport_session) = &mut transport_session {
            **transport_session = Some(id.clone());
        }
        Ok(Arc::new(FlightSession { id, context }))
    }

    pub(super) fn response<T>(
        &self,
        session: &FlightSession,
        value: T,
    ) -> Result<Response<T>, Status> {
        let mut response = Response::new(value);
        response.metadata_mut().insert(
            "set-cookie",
            format!(
                "{SESSION_COOKIE_NAME}={}; Path=/; HttpOnly; SameSite=Strict",
                session.id
            )
            .parse()
            .map_err(internal)?,
        );
        Ok(response)
    }

    pub(super) async fn get_options(session: &FlightSession) -> Result<SessionOptions, Status> {
        let manager = session
            .context
            .extension::<CatalogManager>()
            .map_err(internal)?;
        Ok(SessionOptions {
            session_options: HashMap::from([
                (
                    "catalog".into(),
                    SessionOptionValue {
                        value: Some(Value::String(
                            manager.default_catalog().map_err(internal)?.to_string(),
                        )),
                    },
                ),
                (
                    "schema".into(),
                    SessionOptionValue {
                        value: Some(Value::String(quote_names_if_needed(&Vec::<String>::from(
                            manager.default_database().map_err(internal)?,
                        )))),
                    },
                ),
            ]),
        })
    }

    pub(super) async fn set_options(
        session: &FlightSession,
        options: SessionOptions,
    ) -> Result<SetSessionOptionsResult, Status> {
        let manager = session
            .context
            .extension::<CatalogManager>()
            .map_err(internal)?;
        let mut result = SetSessionOptionsResult::default();
        // Catalog must be applied before schema regardless of protobuf map order.
        let mut options = options.session_options.into_iter().collect::<Vec<_>>();
        options.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, option) in options {
            if !matches!(name.as_str(), "catalog" | "schema") {
                result.errors.insert(name, SessionOptionError { value: 1 });
                continue;
            }
            let value = match option.value {
                Some(Value::String(value)) => value,
                _ => {
                    result.errors.insert(name, SessionOptionError { value: 2 });
                    continue;
                }
            };
            let set = if name == "catalog" {
                manager.set_default_catalog(value).map_err(invalid)
            } else {
                // The same SQL identifier parser used for table/schema names handles
                // nested namespaces and quoted dots without string interpolation.
                match sail_sql_analyzer::parser::parse_object_name(&value) {
                    Ok(parts) => manager
                        .set_default_database(
                            parts.0.items().map(|p| p.value.clone()).collect::<Vec<_>>(),
                        )
                        .await
                        .map_err(invalid),
                    Err(error) => Err(invalid(error)),
                }
            };
            if set.is_err() {
                result.errors.insert(name, SessionOptionError { value: 2 });
            }
        }
        Ok(result)
    }
}
