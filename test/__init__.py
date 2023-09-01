import requests

url = "http://192.168.66.66:9090"
# code
try:
	r = requests.get(url, timeout=1)
	r.raise_for_status()
	print(r.status_code)
except requests.exceptions.MissingSchema as errmiss:
	print("Missing schema: include http or https")
except requests.exceptions.ReadTimeout as errrt:
	print("Time out")
