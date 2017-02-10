import requests
import json
import urllib2
import urllib
import pandas as pd

if __name__ == '__main__':
	with open('appcreds.txt', 'r') as credfile:
		uid, secret = credfile.read().splitlines()

	r = requests.post("https://api.intra.42.fr/oauth/token", data={'grant_type': 'client_credentials', 'client_id': uid, 'client_secret': secret})
	r.raise_for_status()
	access_token = json.loads(r.text)['access_token']
	print(access_token)
	df = pd.read_csv('users_url.csv')
	people = []
	for i in range(200, 301):
	#for i in range(1, len(df)):
		print(i)
		
		#python3 implementation
		#f = urllib.request.urlopen(df['url'][i]+('?access_token=%s' % (access_token)))
		
		#python2 implementation
		f = urllib2.urlopen(df['url'][i]+('?access_token=%s' % (access_token)))
		res = json.loads(f.read())
		#print(res)
		people.append(res)
	
	#output as json
	with open('users_info.txt', 'w') as outfile:
	    json.dump(people, outfile)
	
	#output as DataFrame
	#df = pd.DataFrame(pd.read_json(json.dumps(people)))
	#df.to_csv('users_info.csv')
	
	#print(pd.read_csv('users_info.csv'))
