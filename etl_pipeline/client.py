import time
import requests
import logging

logger = logging.getLogger(__name__)

class EcomAPIClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        
        # Token Storage
        self.access_token = None
        self.refresh_token = None
        self.token_type = "Bearer"
        self.expires_at = 0  
        
        # Optimization: Use Session for connection pooling
        self.session = requests.Session()

    def _get_url(self, endpoint):
        """Helper to handle path joining cleanly."""
        if endpoint.startswith("http"):
            return endpoint
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def login(self):
        """Performs initial login to get tokens."""
        logger.info(f"Authenticating user: {self.username}...")
        login_url = self._get_url("auth/login") 
        
        try:
            response = self.session.post(login_url, json={
                "username": self.username,
                "password": self.password
            })
            response.raise_for_status()
            self._update_tokens(response.json())
            logger.info("Login successful.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Login failed: {e}")
            raise

    def _update_tokens(self, data):
        """Parses response to update tokens and expiry time."""
        self.access_token = data.get("access_token")
        self.refresh_token = data.get("refresh_token")
        self.token_type = data.get("token_type", "Bearer")
        
        # Calculate expiry: Current Time + Duration - Buffer (10s)
        expires_in = data.get("expires_in", 3600) 
        self.expires_at = time.time() + expires_in - 10

    def refresh_access_token(self):
        """Uses refresh_token to get a new access_token."""
        if not self.refresh_token:
            logger.warning("No refresh token available. Attempting fresh login.")
            return self.login()

        logger.info("Access token expired. Refreshing...")
        # ADJUST THIS ENDPOINT to match your API (e.g., /auth/refresh)
        refresh_url = self._get_url("refresh")
        
        try:
            # Note: Verify if your API expects refresh_token in Body or Headers
            response = self.session.post(refresh_url, json={
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token"
            })
            response.raise_for_status()
            self._update_tokens(response.json())
            logger.info("Token refresh successful.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Refresh failed: {e}. Re-initiating login.")
            self.login()

    def _ensure_valid_token(self):
        """
        Proactive check:
        1. Logs in if no token exists.
        2. Refreshes if token is expired (based on time).
        """
        if not self.access_token:
            self.login()
            return

        if time.time() > self.expires_at:
            self.refresh_access_token()

    def fetch_data(self, endpoint, params=None):
        """
        Fetches data with automatic auth handling (Proactive + Reactive).
        """
        # 1. Proactive: Check if we know the token is expired
        self._ensure_valid_token()

        url = self._get_url(endpoint)
        headers = {
            "Authorization": f"{self.token_type} {self.access_token}",
            "Content-Type": "application/json"
        }

        try:
            response = self.session.get(url, headers=headers, params=params)

            # 2. Reactive: If server returns 401 Unauthorized despite our checks
            if response.status_code == 401:
                logger.warning("Received 401. Token likely revoked. Refreshing and retrying...")
                self.refresh_access_token()
                
                # Update header with new token
                headers["Authorization"] = f"{self.token_type} {self.access_token}"
                response = self.session.get(url, headers=headers, params=params)

            return response

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            # Return a generic error response or re-raise
            raise