from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import os


def get_session():
    client_id = os.environ["SH_CLIENT_ID"]
    client_secret = os.environ["SH_CLIENT_SECRET"]

    client = BackendApplicationClient(client_id=client_id)
    session = OAuth2Session(client=client)

    session.fetch_token(
        token_url="https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        client_secret=client_secret,
        include_client_id=True,
    )

    return session
