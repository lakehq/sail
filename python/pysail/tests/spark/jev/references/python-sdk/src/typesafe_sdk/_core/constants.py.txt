"""Internal protocol and logging constants."""

MAX_ERROR_BODY_LENGTH = 200

SYSTEM_ONE_PATH = "/v1/systemone"
MODELS_PATH = "/v1/models"
SDK_NAME = "typesafe-sdk"
LOGGER_NAME = "typesafe_sdk"
JSON_CONTENT_TYPE = "application/json"

AUTHORIZATION_HEADER = "Authorization"
ACCEPT_HEADER = "Accept"
CONTENT_TYPE_HEADER = "Content-Type"
USER_AGENT_HEADER = "User-Agent"
SDK_HEADER = "X-TypeSafe-SDK"
RUNTIME_HEADER = "X-TypeSafe-Runtime"
RETRY_COUNT_HEADER = "X-TypeSafe-Retry-Count"
REQUEST_ID_HEADER = "x-typesafe-request-id"
RETRY_AFTER_HEADER = "retry-after"
RETRY_AFTER_MS_HEADER = "retry-after-ms"
SECRET_HEADERS = frozenset({"authorization", "proxy-authorization", "x-api-key", "api-key", "cookie", "set-cookie"})
