import requests
import json

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class LookerApi(object):
    def __init__(self, token, secret, host):
        self.token = token
        self.secret = secret
        self.host = 'https://' + host + '.looker.com:19999/api/3.0/'

        self.session = requests.Session()
        self.session.verify = False

        self.auth()

    def auth(self):
        url = '{}{}'.format(self.host,'login')
        params = {'client_id':self.token,
                  'client_secret':self.secret
                  }
        r = self.session.post(url,params=params)
        access_token = r.json().get('access_token')
        self.session.headers.update({'Authorization': 'token {}'.format(access_token)})

    def update_connection(self, connection_name, host, user, pw):
        url = '{}/connections/{}'.format(self.host, connection_name)
        params = {"host": host, "username": user, "password": pw}
        r = self.session.patch(url, params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.json()