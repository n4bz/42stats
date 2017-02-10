import requests
import json
import urllib
import urllib2
import pandas as pd
import datetime
import time
import gspread
import os
import logging
from oauth2client.service_account import ServiceAccountCredentials

from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig()
sched = BlockingScheduler()

@sched.scheduled_job('cron', hour=22, minute=16)
def scheduled_job():

	print('Job started')
	#Parse user info by each url from users_url and save to a file

	with open('appcreds.txt', 'r') as credfile:
		uid, secret = credfile.read().splitlines()

	r = requests.post("https://api.intra.42.fr/oauth/token", data={'grant_type': 'client_credentials', 'client_id': uid, 'client_secret': secret})
	r.raise_for_status()
	access_token = json.loads(r.text)['access_token']
	print(access_token)
	df = pd.read_csv('users_url.csv')
	people = []
	#for i in range(250, 301):
	for i in range(1, len(df)):
		print(i)
		#python3 implementation
		#f = urllib.request.urlopen(df['url'][i]+('?access_token=%s' % (access_token)))
		
		#python2 implementation
		f = urllib2.urlopen(df['url'][i]+('?access_token=%s' % (access_token)))
		res = json.loads(f.read())
		#print(res)
		people.append(res)
	with open('users_info.txt', 'w') as outfile:
		json.dump(people, outfile)

	print('User info saved')
	#Create and update Leaderboards

	#load user_info
	with open('users_info.txt', 'r') as file:
		js0 = json.loads(file.read())

	#from deeper json bring info (level, start_date) to top for Piscine_C and 42 courses
	id_42 = 1
	id_pisc_c = 4
	level_42 = []
	skills_42 = []
	start_date_42 = [[], []]
	level_pisc_c = []
	skills_pisc_c = []
	counter = 0
	t0 = datetime.datetime(2016, 9, 1)
	for item in js0:
		flag = 0
		flag2 = 0
		for el in item['cursus_users']:
			if el['cursus_id'] == 4:
				level_pisc_c.append(round(el['level'], 2))
				skills_pisc_c.append(el['skills'])
				flag += 1
			elif el['cursus_id'] == 1:
				level_42.append(round(el['level'], 2))			
				skills_42.append(el['skills'])
				if el['begin_at'] != None and len(el['begin_at']) > 10:
					t = datetime.datetime.strptime(el['begin_at'][:10], '%Y-%m-%d')
					start_date_42[0].append((t - t0).total_seconds())
					start_date_42[1].append(el['begin_at'][:10])
				else:
					start_date_42[1].append("")
					start_date_42[0].append(-1)
				flag += 10
				flag2 = 1
		if flag < 10:
			level_42.append(-1)
			skills_42.append("")
			start_date_42[1].append("")
			start_date_42[0].append(-1)
			flag2 = 1
		if flag % 10 == 0:
			level_pisc_c.append(-1)
			skills_pisc_c.append("")

	#make a dataframe from initial json
	df0 = pd.DataFrame(js0)

	#Add levels and start_date to DaaFrame
	df0.loc[:,'level_42'] = pd.Series(level_42, index=df0.index)
	df0.loc[:,'level_C'] = pd.Series(level_pisc_c, index=df0.index)
	df0.loc[:,'start_date'] = pd.Series(start_date_42[1], index=df0.index)
	df0.loc[:,'secs_start_date'] = pd.Series(start_date_42[0], index=df0.index)

	#Filter relevent data for Piscine_C and 42 courses
	df0 = df0[df0['pool_year'] >= '2016']
	df_42 = df0[df0['secs_start_date'] > 0]
	df_C = df0[df0['level_C'] >= 0]

	#Sort data and reindex
	df_42 = df_42.sort_values(['level_42', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
	df_C = df_C.sort_values(['level_C', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
	df_42['place'] = df_42.index + 1
	df_C['place'] = df_C.index + 1

	#Select only relevent rows to display/record
	df_42 = df_42[['place', 'login', 'last_name', 'first_name', 'level_42', 'start_date', 'pool_month']]
	df_C = df_C[['place', 'login', 'last_name', 'first_name', 'level_C', 'pool_month', 'pool_year']]

	#output 42 rank table to csv
	#df_42.to_csv('leaderboard_42.csv')
	#df_C.to_csv('leaderboard_C.csv')
	#df_42.to_json('leaderboard_42.json', orient='records')


	print('Leaderboard made')

	#Connect to Google Spreadsheet
	scope =['https://spreadsheets.google.com/feeds']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('42stats-30ad16650adf.json', scope)
	gc = gspread.authorize(credentials)

	#Upadte 42 cursus
	sh = gc.open('leaderboard_42')
	worksheet = sh.get_worksheet(0)
	keys = list(df_42)
	cols = len(keys)
	rows = len(df_42['place'])
	header_list = worksheet.range(1, 1, 1, cols)
	for i in range(len(keys)):
		header_list[i].value = keys[i]
	cell_list = worksheet.range(2, 1, rows + 1, cols)
	for i in range(rows):
		for j in range(cols):
			cell_list[j + i * cols].value = df_42.iloc[i, j]
	# Update in batch
	worksheet.update_cells(header_list)
	worksheet.update_cells(cell_list)

	#Upadte C cursus
	sh = gc.open('leaderboard_c')
	worksheet = sh.get_worksheet(0)
	keys = list(df_C)
	cols = len(keys)
	rows = len(df_C['place'])
	header_list = worksheet.range(1, 1, 1, cols)
	for i in range(len(keys)):
		header_list[i].value = keys[i]
	cell_list = worksheet.range(2, 1, rows + 1, cols)
	for i in range(rows):
		for j in range(cols):
			cell_list[j + i * cols].value = df_C.iloc[i, j]
	# Update in batch
	worksheet.update_cells(header_list)
	worksheet.update_cells(cell_list)
	print('Job completed')

sched.start()