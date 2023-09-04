import json
import requests


def get_token(token_url, username, password):
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
        auth_data = {
            'email': username,
            'password': password
        }
        resp = requests.post(token_url, data=json.dumps(auth_data), headers=headers).json()

        return resp



url = "http://185.255.89.2/gcms/api/TreeCoverLossRaster/"
auth_token = get_token(url, 'mortezakhazaei1370@gmail.com', 'm3541532')
print(auth_token)
# headers = {'Accept': 'application/json', 'Authorization': 'Token {}'.format(auth_token)}
# # code
# try:
# 	r = requests.get(url, timeout=1)
# 	r.raise_for_status()
# 	print(r.status_code)
# except requests.exceptions.MissingSchema as errmiss:
# 	print("Missing schema: include http or https")
# except requests.exceptions.ReadTimeout as errrt:
# 	print("Time out")