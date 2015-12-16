import time
import hmac
import hashlib
import base64

from requests.auth import AuthBase


def dict_compare(new_dict, old_dict):
    d1_keys = set(new_dict.keys())
    d2_keys = set(old_dict.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o: (new_dict[o], old_dict[o]) for o in intersect_keys if new_dict[o] != old_dict[o]}
    same = set(o for o in intersect_keys if new_dict[o] == old_dict[o])
    return added, removed, modified, same


class CoinbaseExchangeAuthentication(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        message = message.encode('utf-8')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest())

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
        })
        return request

