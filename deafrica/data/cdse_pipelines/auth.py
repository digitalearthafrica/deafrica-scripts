from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session
import os
import threading
import time

TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE"
    "/protocol/openid-connect/token"
)

# Re-authenticate this many seconds before the server-stated expiry.
EXPIRY_MARGIN = 120


class RefreshingOAuth2Session(OAuth2Session):
    "Class for an OAuth2Session that automatically refreshes its token when it is stale or expired."

    def __init__(self, client_id, client_secret, token_url=TOKEN_URL):
        super().__init__(client=BackendApplicationClient(client_id=client_id))
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = token_url
        self._lock = threading.Lock()
        self._local = threading.local()
        self._generation = 0
        self._authenticate(None)

    def _authenticate(self, seen_generation):
        with self._lock:
            if seen_generation is not None and seen_generation != self._generation:
                return  # another thread already re-authenticated
            self._local.authenticating = True
            try:
                super().fetch_token(
                    token_url=self._token_url,
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    include_client_id=True,
                    timeout=30,
                )
            finally:
                self._local.authenticating = False
            self._generation += 1

    def _stale(self):
        expires_at = (self.token or {}).get("expires_at")
        return expires_at is None or time.time() >= float(expires_at) - EXPIRY_MARGIN

    def request(self, method, url, *args, withhold_token=False, **kwargs):
        # fetch_token() issues its own request through this method - send it
        # straight out, or we recurse into the lock we are already holding.
        # Some requests_oauthlib versions omit withhold_token here, so the
        # thread-local flag is what actually does the work.
        if withhold_token or getattr(self._local, "authenticating", False):
            return super().request(method, url, *args, withhold_token=True, **kwargs)

        generation = self._generation
        if self._stale():
            self._authenticate(generation)
            generation = self._generation

        try:
            resp = super().request(method, url, *args, **kwargs)
        except TokenExpiredError:
            self._authenticate(generation)
            return super().request(method, url, *args, **kwargs)

        # Clock skew or server-side revocation: expires_at can still look healthy.
        if resp.status_code == 401:
            self._authenticate(generation)
            resp = super().request(method, url, *args, **kwargs)

        return resp


def get_session():
    client_id = os.environ["SH_CLIENT_ID"]
    client_secret = os.environ["SH_CLIENT_SECRET"]

    return RefreshingOAuth2Session(client_id, client_secret)
