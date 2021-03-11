import requests

class GlobusAuthManager:
    def __init__(self, client_key, client_secret):
        self.auth = (client_key, client_secret)
        self.client_key = client_key
        self.client_secret = client_secret

    def scope_id_from_uri(self, uri):
        r = requests.get('https://auth.globus.org/v2/api/scopes',
                         auth=self.auth,
                         params={'scope_strings': uri})
        return r.json()
