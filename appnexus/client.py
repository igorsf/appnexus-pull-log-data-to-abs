__author__ = 'igorsf'

import os
import requests
import json
import pickle
import logging

logger = logging.getLogger(__name__)

class AppNexusClient(object):

    def __init__(self, username, password, cookie_file='./.auth'):
        self.username = username
        self.password = password
        self.cookie_file = cookie_file

    def request(self, url, params=None, **kwargs):

        cookie_jar = self._load_cookies()

        if not cookie_jar or not self._check_auth(cookie_jar):
            cookie_jar = self._do_new_auth()

        r = requests.get(url, params=params, cookies=cookie_jar, **kwargs)
        return r

    def _do_new_auth(self):

        payload = {
            'auth': {
                'username': self.username,
                'password': self.password
            }
        }

        url = 'https://api.appnexus.com/auth'

        r = requests.post(url, data=json.dumps(payload))

        resp = json.loads(r.content)

        if resp['response'].get('status', False) != "OK":
            logger.debug("Authentication failed with status code {0}".format(resp['response']))
            return False

        logger.info("Successfully authenticated")
        cookie_jar = r.cookies
        self._save_cookies(cookie_jar)

        return cookie_jar

    def _check_auth(self, cookie_jar):
        r = requests.get('https://api.appnexus.com/user?current', cookies=cookie_jar)
        resp = json.loads(r.content)
        if resp['response'].get('status', False) != "OK":
            return False
        else:
            return True

    def _save_cookies (self, cookie_jar):
        if os.path.exists(self.cookie_file):
                os.remove(self.cookie_file)

        f = open(self.cookie_file, 'wb')
        pickle.dump(cookie_jar, f)

    def _load_cookies (self):
        if os.path.exists(self.cookie_file):
            f = open(self.cookie_file, 'rb')
            cookie_jar = pickle.load(f)
            return cookie_jar
        else:
            return False