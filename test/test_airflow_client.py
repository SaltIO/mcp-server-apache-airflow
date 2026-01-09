"""Tests for the airflow client authentication module."""

import os
import sys
from unittest.mock import patch, MagicMock, Mock

import pytest
from airflow_client.client import ApiClient


class TestAirflowClientAuthentication:
    """Test cases for airflow client authentication configuration."""

    def test_basic_auth_configuration(self):
        """Test that basic authentication is configured correctly."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            # Clear any cached modules
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            # Re-import after setting environment
            from src.airflow.airflow_client import api_client, configuration

            # Verify configuration
            assert configuration.host == "http://localhost:8080/api/v1"
            assert configuration.username == "testuser"
            assert configuration.password == "testpass"
            assert isinstance(api_client, ApiClient)

    def test_jwt_token_auth_configuration(self):
        """Test that JWT token authentication is configured correctly."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.jwt.token",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            # Clear any cached modules
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            # Re-import after setting environment
            from src.airflow.airflow_client import api_client, configuration

            # Verify configuration
            assert configuration.host == "http://localhost:8080/api/v1"
            assert configuration.api_key == {"Authorization": "Bearer test.jwt.token"}
            assert configuration.api_key_prefix == {"Authorization": ""}
            assert isinstance(api_client, ApiClient)

    def test_jwt_token_takes_precedence_over_basic_auth(self):
        """Test that JWT token takes precedence when both auth methods are provided."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_JWT_TOKEN": "test.jwt.token",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            # Clear any cached modules
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            # Re-import after setting environment
            from src.airflow.airflow_client import api_client, configuration

            # Verify JWT token is used (not basic auth)
            assert configuration.host == "http://localhost:8080/api/v1"
            assert configuration.api_key == {"Authorization": "Bearer test.jwt.token"}
            assert configuration.api_key_prefix == {"Authorization": ""}
            # Basic auth should not be set when JWT is present
            assert not hasattr(configuration, "username") or configuration.username is None
            assert not hasattr(configuration, "password") or configuration.password is None
            assert isinstance(api_client, ApiClient)

    def test_no_auth_configuration(self):
        """Test that configuration works with no authentication (for testing/development)."""
        with patch.dict(os.environ, {"AIRFLOW_HOST": "http://localhost:8080", "AIRFLOW_API_VERSION": "v1"}, clear=True):
            # Clear any cached modules
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            # Re-import after setting environment
            from src.airflow.airflow_client import api_client, configuration

            # Verify configuration
            assert configuration.host == "http://localhost:8080/api/v1"
            # No auth should be set
            assert not hasattr(configuration, "username") or configuration.username is None
            assert not hasattr(configuration, "password") or configuration.password is None
            # api_key might be an empty dict by default, but should not have Authorization
            assert "Authorization" not in getattr(configuration, "api_key", {})
            assert isinstance(api_client, ApiClient)

    def test_environment_variable_parsing(self):
        """Test that environment variables are parsed correctly."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "https://airflow.example.com:8080",
                "AIRFLOW_JWT_TOKEN": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            # Clear any cached modules
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            # Re-import after setting environment
            from src.airflow.airflow_client import configuration
            from src.envs import AIRFLOW_API_VERSION, AIRFLOW_HOST, AIRFLOW_JWT_TOKEN

            # Verify environment variables are parsed correctly
            assert AIRFLOW_HOST == "https://airflow.example.com:8080"
            assert AIRFLOW_JWT_TOKEN == "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
            assert AIRFLOW_API_VERSION == "v2"

            # Verify configuration uses parsed values
            assert configuration.host == "https://airflow.example.com:8080/api/v2"
            assert configuration.api_key == {"Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."}
            assert configuration.api_key_prefix == {"Authorization": ""}

    def test_environment_variable_parsing_with_custom_path(self):
        """Test that custom paths in AIRFLOW_HOST are preserved."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "https://airflow.example.com:8080/custom",
                "AIRFLOW_JWT_TOKEN": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            # Clear any cached modules
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            # Re-import after setting environment
            from src.airflow.airflow_client import configuration
            from src.envs import AIRFLOW_HOST

            # Path should be preserved, not stripped
            assert AIRFLOW_HOST == "https://airflow.example.com:8080/custom"
            assert configuration.host == "https://airflow.example.com:8080/custom/api/v2"


class TestV1APIAuthentication:
    """Test cases for v1 API authentication logic."""

    def test_v1_jwt_provided(self):
        """Test v1 API with JWT token provided - should use JWT."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.jwt.token",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration

            assert configuration.host == "http://localhost:8080/api/v1"
            assert configuration.api_key == {"Authorization": "Bearer test.jwt.token"}
            assert isinstance(api_client, ApiClient)

    def test_v1_username_password_no_jwt(self):
        """Test v1 API with username/password but no JWT - should use basic auth."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration

            assert configuration.host == "http://localhost:8080/api/v1"
            assert configuration.username == "testuser"
            assert configuration.password == "testpass"
            assert isinstance(api_client, ApiClient)

    def test_v1_no_credentials(self):
        """Test v1 API with no credentials - should work without auth."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration

            assert configuration.host == "http://localhost:8080/api/v1"
            assert isinstance(api_client, ApiClient)


class TestV2PlusAPIAuthentication:
    """Test cases for v2+ API authentication logic."""

    def test_v2_only_jwt_provided(self):
        """Test v2 API with only JWT provided - should use JWT, no refresh capability."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.jwt.token",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration, refresh_token_if_needed

            assert configuration.host == "http://localhost:8080/api/v2"
            assert configuration.api_key == {"Authorization": "Bearer test.jwt.token"}
            assert isinstance(api_client, ApiClient)

            # Should not be able to refresh (no username/password)
            assert refresh_token_if_needed() is False

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_v2_jwt_and_username_password(self, mock_pool_manager):
        """Test v2 API with JWT and username/password - should use JWT, can refresh."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.jwt.token",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration, refresh_token_if_needed

            assert configuration.host == "http://localhost:8080/api/v2"
            assert configuration.api_key == {"Authorization": "Bearer test.jwt.token"}
            assert isinstance(api_client, ApiClient)

            # Should be able to refresh (has username/password)
            # Mock successful token refresh
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.data = b'{"access_token": "new.token.here"}'
            mock_http = MagicMock()
            mock_http.request.return_value = mock_response
            mock_pool_manager.return_value = mock_http

            result = refresh_token_if_needed()
            assert result is True
            assert configuration.api_key == {"Authorization": "Bearer new.token.here"}

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_v2_username_password_no_jwt(self, mock_pool_manager):
        """Test v2 API with username/password but no JWT - should fetch token first."""
        # Mock successful token fetch
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.data = b'{"access_token": "fetched.token.here"}'
        mock_http = MagicMock()
        mock_http.request.return_value = mock_response
        mock_pool_manager.return_value = mock_http

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration, refresh_token_if_needed

            # Should have called the token endpoint
            assert mock_http.request.called
            call_args = mock_http.request.call_args
            assert call_args[0][0] == 'POST'
            assert call_args[0][1] == "http://localhost:8080/auth/token"
            # Check the body contains the credentials
            body = call_args[1]['body'].decode('utf-8')
            import json
            body_data = json.loads(body)
            assert body_data == {"username": "testuser", "password": "testpass"}

            assert configuration.host == "http://localhost:8080/api/v2"
            assert configuration.api_key == {"Authorization": "Bearer fetched.token.here"}
            assert isinstance(api_client, ApiClient)

            # Should be able to refresh
            mock_response.data = b'{"access_token": "refreshed.token.here"}'
            result = refresh_token_if_needed()
            assert result is True
            assert configuration.api_key == {"Authorization": "Bearer refreshed.token.here"}

    def test_v2_no_credentials(self):
        """Test v2 API with no credentials - should raise RuntimeError."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            with pytest.raises(RuntimeError, match="v2 API requires JWT token authentication"):
                from src.airflow.airflow_client import configuration

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_v2_fetch_token_failure(self, mock_pool_manager):
        """Test v2 API when token fetch fails - should raise RuntimeError."""
        # Mock failed token fetch
        mock_http = MagicMock()
        mock_http.request.side_effect = Exception("Connection error")
        mock_pool_manager.return_value = mock_http

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            with pytest.raises(RuntimeError, match="Cannot proceed without token"):
                from src.airflow.airflow_client import configuration

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_v3_api_behavior(self, mock_pool_manager):
        """Test v3 API follows same logic as v2."""
        # Mock successful token fetch
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.data = b'{"access_token": "v3.token.here"}'
        mock_http = MagicMock()
        mock_http.request.return_value = mock_response
        mock_pool_manager.return_value = mock_http

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v3",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import api_client, configuration

            assert configuration.host == "http://localhost:8080/api/v3"
            assert configuration.api_key == {"Authorization": "Bearer v3.token.here"}
            assert isinstance(api_client, ApiClient)


class TestTokenRefresh:
    """Test cases for token refresh functionality."""

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_refresh_token_success(self, mock_pool_manager):
        """Test successful token refresh."""
        # Mock successful token refresh
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.data = b'{"access_token": "refreshed.token"}'
        mock_http = MagicMock()
        mock_http.request.return_value = mock_response
        mock_pool_manager.return_value = mock_http

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "initial.token",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import configuration, refresh_token_if_needed, api_client

            # Initial token
            assert configuration.api_key == {"Authorization": "Bearer initial.token"}

            # Refresh token
            result = refresh_token_if_needed()
            assert result is True
            assert configuration.api_key == {"Authorization": "Bearer refreshed.token"}
            assert api_client.configuration.api_key == {"Authorization": "Bearer refreshed.token"}

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_refresh_token_failure(self, mock_pool_manager):
        """Test token refresh failure."""
        # Mock failed token refresh
        mock_http = MagicMock()
        mock_http.request.side_effect = Exception("Token fetch failed")
        mock_pool_manager.return_value = mock_http

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "initial.token",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import configuration, refresh_token_if_needed

            # Initial token should remain
            assert configuration.api_key == {"Authorization": "Bearer initial.token"}

            # Refresh should fail
            result = refresh_token_if_needed()
            assert result is False
            # Token should remain unchanged
            assert configuration.api_key == {"Authorization": "Bearer initial.token"}

    def test_refresh_token_v1_api(self):
        """Test that token refresh doesn't work for v1 API."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.token",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import refresh_token_if_needed

            # Should return False immediately for v1
            assert refresh_token_if_needed() is False


class TestCallWithTokenRefresh:
    """Test cases for call_with_token_refresh wrapper."""

    @patch('src.airflow.airflow_client.urllib3.PoolManager')
    def test_call_with_token_refresh_401_retry(self, mock_pool_manager):
        """Test that 401 errors trigger token refresh and retry."""
        # Mock successful token refresh
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.data = b'{"access_token": "refreshed.token"}'
        mock_http = MagicMock()
        mock_http.request.return_value = mock_response
        mock_pool_manager.return_value = mock_http

        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "initial.token",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import call_with_token_refresh

            # Mock API call that fails with 401, then succeeds
            mock_api_call = Mock(side_effect=[
                type('ApiException', (Exception,), {'status': 401})(),
                "success_result"
            ])

            result = call_with_token_refresh(mock_api_call, "arg1", kwarg1="value1")

            assert result == "success_result"
            assert mock_api_call.call_count == 2
            # Should have called token refresh
            assert mock_http.request.called

    def test_call_with_token_refresh_non_401_error(self):
        """Test that non-401 errors are not retried."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.token",
                "AIRFLOW_API_VERSION": "v2",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import call_with_token_refresh

            # Mock API call that fails with 500
            mock_api_call = Mock(side_effect=type('ApiException', (Exception,), {'status': 500})())

            with pytest.raises(Exception):
                call_with_token_refresh(mock_api_call)

            # Should only be called once (no retry)
            assert mock_api_call.call_count == 1

    def test_call_with_token_refresh_v1_no_refresh(self):
        """Test that v1 API doesn't attempt token refresh on 401."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW_HOST": "http://localhost:8080",
                "AIRFLOW_JWT_TOKEN": "test.token",
                "AIRFLOW_USERNAME": "testuser",
                "AIRFLOW_PASSWORD": "testpass",
                "AIRFLOW_API_VERSION": "v1",
            },
            clear=True,
        ):
            modules_to_clear = ["src.envs", "src.airflow.airflow_client"]
            for module in modules_to_clear:
                if module in sys.modules:
                    del sys.modules[module]

            from src.airflow.airflow_client import call_with_token_refresh

            # Mock API call that fails with 401
            mock_api_call = Mock(side_effect=type('ApiException', (Exception,), {'status': 401})())

            with pytest.raises(Exception):
                call_with_token_refresh(mock_api_call)

            # Should only be called once (no retry for v1)
            assert mock_api_call.call_count == 1
