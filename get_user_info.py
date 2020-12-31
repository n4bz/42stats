import requests
import json
import urllib
import pandas as pd
from threading import Thread
import time

if __name__ == '__main__':
	with open('appcreds.txt', 'r') as credfile:
		uid, secret = credfile.read().splitlines()

	r = requests.post("https://api.intra.42.fr/oauth/token", data={'grant_type': 'client_credentials', 'client_id': uid, 'client_secret': secret})
	r.raise_for_status()
	access_token = json.loads(r.text)['access_token']
	print(access_token)
	df = pd.read_csv('users_url.csv')
	people = []
	
	#Threding function to append users info to output list 'people'
	def recrod_info(user_url):
			
		#python3 implementation
		f = urllib.request.urlopen(user_url+('?access_token=%s' % (access_token)))
		
		#python2 implementation
		#f = urllib2.urlopen(user_url+('?access_token=%s' % (access_token)))
		
		people.append(json.loads(f.read()))

	
	#Multi-thread recording with external loop reducing number of threads to not overflood API with requests
	req_slower = 20
	length = len(df)
	for count in range(req_slower):
		threads = []
		print("Recording: %d%%" % int(count * 100 / req_slower))
		
		#Multi-threading to speed-up user info gathering from API
		for i in range((length * count) // req_slower, (length * (count + 1)) // req_slower):
			count += 1
			process = Thread(target=recrod_info, args=[df['url'][i]])
			process.start()
			time.sleep(1/2)
			threads.append(process)

		#Join is improtant before starting another process threading batch or world will go topsy turvy	
		for process in threads:
			process.join()
	print("Recording completed")

	#output as json
	with open('users_info.txt', 'w') as outfile:
		json.dump(people, outfile)
	
	#output as DataFrame
	#df = pd.DataFrame(pd.read_json(json.dumps(people)))
	#df.to_csv('users_info.csv')
	
	#print(pd.read_csv('users_info.csv'))

