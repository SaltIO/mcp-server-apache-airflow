from airflow_client.client import ApiClient, Configuration
import urllib3
import json
import logging
import re

from src.envs import (
    AIRFLOW_API_VERSION,
    AIRFLOW_HOST,
    AIRFLOW_JWT_TOKEN,
    AIRFLOW_PASSWORD,
    AIRFLOW_USERNAME,
)

logger = logging.getLogger(__name__)


def _is_v2_or_greater(api_version: str) -> bool:
    """
    Check if the API version is v2 or greater.

    Args:
        api_version: API version string (e.g., "v1", "v2", "v3")

    Returns:
        True if v2 or greater, False otherwise
    """
    # Extract version number from string like "v1", "v2", etc.
    match = re.match(r'v(\d+)', api_version.lower())
    if match:
        version_num = int(match.group(1))
        return version_num >= 2
    return False


class JWTConfiguration(Configuration):
    """Configuration subclass that supports JWT token authentication."""

    def __init__(self, jwt_token=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._jwt_token = jwt_token

    def update_token(self, new_token: str):
        """Update the JWT token dynamically."""
        self._jwt_token = new_token
        # Update the api_key to reflect the new token
        self.api_key = {"Authorization": f"Bearer {new_token}"}
        self.api_key_prefix = {"Authorization": ""}

    def auth_settings(self):
        """Override auth_settings to support JWT token authentication."""
        auth = {}
        if self._jwt_token:
            # Use 'Basic' as the key (expected by endpoint OpenAPI spec) but provide Bearer token
            # The endpoint's OpenAPI spec lists 'Basic' and 'Kerberos', but API requires Bearer token
            auth['Basic'] = {
                'type': 'bearer',
                'in': 'header',
                'key': 'Authorization',
                'value': f"Bearer {self._jwt_token}"
            }
        elif self.username is not None and self.password is not None:
            # Fallback to Basic auth if no JWT token
            auth['Basic'] = {
                'type': 'basic',
                'in': 'header',
                'key': 'Authorization',
                'value': self.get_basic_auth_token()
            }
        return auth


def fetch_jwt_token(airflow_host: str, username: str, password: str) -> str:
    """
    Fetch a JWT token from Airflow's authentication endpoint.

    Args:
        airflow_host: The Airflow host URL (e.g., https://test.cmdrvl.com/airflow)
        username: Username for authentication
        password: Password for authentication

    Returns:
        JWT token string

    Raises:
        Exception: If token fetch fails
    """
    auth_url = f"{airflow_host.rstrip('/')}/auth/token"

    # Disable SSL warnings if needed
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Create a PoolManager with SSL verification disabled
    http = urllib3.PoolManager(cert_reqs='CERT_NONE')

    try:
        response = http.request(
            'POST',
            auth_url,
            body=json.dumps({"username": username, "password": password}).encode('utf-8'),
            headers={"Content-Type": "application/json"},
            timeout=urllib3.Timeout(connect=10, read=10)
        )

        if response.status != 200:
            raise Exception(f"Token fetch failed with status {response.status}: {response.data.decode('utf-8')}")

        # The response should contain the token
        # Format may vary, but typically it's in 'access_token' or 'token' field
        token_data = json.loads(response.data.decode('utf-8'))
        token = token_data.get('access_token') or token_data.get('token')

        if not token:
            raise ValueError(f"Token not found in response: {token_data}")

        logger.info("Successfully fetched new JWT token from Airflow")
        return token

    except Exception as e:
        logger.error(f"Failed to fetch JWT token from {auth_url}: {e}")
        raise


# Create a configuration and API client
# The fix from PR: use string concatenation instead of urljoin to avoid path truncation
# This ensures the full path is preserved when AIRFLOW_HOST includes a base path like /airflow
airflow_host = AIRFLOW_HOST.rstrip("/")
api_host = f"{airflow_host}/api/{AIRFLOW_API_VERSION}"

is_v2_plus = _is_v2_or_greater(AIRFLOW_API_VERSION)
initial_jwt_token = AIRFLOW_JWT_TOKEN
has_username_password = bool(AIRFLOW_USERNAME and AIRFLOW_PASSWORD)

# Authentication logic based on API version
if is_v2_plus:
    # v2+ API: Always use JWT token
    # - If JWT provided, use it
    # - If no JWT but username/password provided, fetch token first
    # - If only JWT provided (no username/password), use it (no refresh capability)
    # - If JWT + username/password provided, use JWT (can refresh on failure)

    if not initial_jwt_token and has_username_password:
        # No JWT provided but username/password available - fetch token
        try:
            logger.info(f"{AIRFLOW_API_VERSION} API detected with username/password but no token - fetching token...")
            initial_jwt_token = fetch_jwt_token(airflow_host, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        except Exception as e:
            logger.error(f"Failed to fetch initial token for {AIRFLOW_API_VERSION} API: {e}")
            raise RuntimeError(f"Cannot proceed without token for {AIRFLOW_API_VERSION} API") from e

    if initial_jwt_token:
        configuration = JWTConfiguration(
            jwt_token=initial_jwt_token,
            host=api_host,
        )
    else:
        # No JWT and no username/password - cannot authenticate for v2+
        raise RuntimeError(f"{AIRFLOW_API_VERSION} API requires JWT token authentication, but no token or credentials provided")

else:
    # v1 API: Prefer JWT if provided, otherwise use basic auth
    if initial_jwt_token:
        configuration = JWTConfiguration(
            jwt_token=initial_jwt_token,
            host=api_host,
        )
    elif has_username_password:
        configuration = Configuration(
            host=api_host,
            username=AIRFLOW_USERNAME,
            password=AIRFLOW_PASSWORD,
        )
    else:
        configuration = Configuration(host=api_host)

# Create API client
api_client = ApiClient(configuration)


def refresh_token_if_needed():
    """
    Refresh the JWT token if it's expired and username/password is available.
    This should be called when a 401 error is encountered.
    Only works for v2+ API and only if username/password is available.

    Returns:
        True if token was refreshed, False otherwise
    """
    if not is_v2_plus:
        # Token refresh only needed for v2+ API
        return False

    if not isinstance(configuration, JWTConfiguration):
        # Not using JWT auth, nothing to refresh
        return False

    if not has_username_password:
        # No credentials to refresh with
        logger.warning("Token expired but no username/password available for refresh")
        return False

    try:
        new_token = fetch_jwt_token(airflow_host, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        configuration.update_token(new_token)
        # Also update the api_client's configuration
        api_client.configuration = configuration
        logger.info("Token refreshed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to refresh token: {e}")
        return False


def call_with_token_refresh(api_call_func, *args, **kwargs):
    """
    Wrapper for API calls that automatically refreshes token on 401 errors.
    Only refreshes for v2+ API when username/password is available.

    Args:
        api_call_func: The API function to call
        *args: Positional arguments for the API call
        **kwargs: Keyword arguments for the API call

    Returns:
        The result of the API call

    Raises:
        The original exception if retry after refresh fails or refresh is not possible
    """
    try:
        return api_call_func(*args, **kwargs)
    except Exception as e:
        # Check if it's a 401 Unauthorized error
        error_code = getattr(e, 'status', None) or getattr(e, 'status_code', None)
        if error_code == 401:
            if is_v2_plus and has_username_password:
                # v2+ API with username/password available - try to refresh
                logger.warning(f"Received 401 Unauthorized, attempting to refresh token...")
                if refresh_token_if_needed():
                    # Retry the call with the new token
                    logger.info("Retrying API call with refreshed token...")
                    return api_call_func(*args, **kwargs)
                else:
                    logger.error("Failed to refresh token, re-raising original exception")
            else:
                # v1 API or no refresh capability - just log and re-raise
                if is_v2_plus:
                    logger.error("401 Unauthorized but cannot refresh token (no username/password available)")
                else:
                    logger.error("401 Unauthorized on v1 API")
        # Re-raise the original exception
        raise
