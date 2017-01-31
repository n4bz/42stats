import requests
import json
import urllib
import pandas as pd

if __name__ == '__main__':
	with open('appcreds.txt', 'r') as credfile:
		uid, secret = credfile.read().splitlines()

	r = requests.post("https://api.intra.42.fr/oauth/token", data={'grant_type': 'client_credentials', 'client_id': uid, 'client_secret': secret})
	r.raise_for_status()
	access_token = json.loads(r.text)['access_token']
	print(access_token)

	url = 'https://api.intra.42.fr/v2/campus/7/users?access_token=%s' % (access_token)
	page = 1
	links = []
	while 1:
	#for i in range(9):
		f = urllib.request.urlopen(url + "&page=" + str(page))
		res = json.loads(f.read())
		print(page)
		if res:
			links += res
		else:
			break
		page += 1
	df = pd.DataFrame(pd.read_json(json.dumps(links)))
	df.to_csv('users_url.csv')
	#print(df)