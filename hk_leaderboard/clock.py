import requests
import json
import urllib
import urllib2
import pandas as pd
import datetime
import time
import math
import gspread
import os
import logging
from threading import Thread
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive   
from oauth2client.service_account import ServiceAccountCredentials

from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig()
sched = BlockingScheduler()


# Test run
#@sched.scheduled_job('cron', year=2017, month=2, day=17, hour=1, minute=20)

# Update schedule
@sched.scheduled_job('cron', hour=1, minute=40)
@sched.scheduled_job('cron', hour=7, minute=40)
# @sched.scheduled_job('cron', hour=19, minute=40)
def scheduled_job():

	print('Job started')

	## Request token to establish API access

	with open('appcreds.txt', 'r') as credfile:
			uid, secret = credfile.read().splitlines()

	r = requests.post("https://api.intra.42.fr/oauth/token", data={'grant_type': 'client_credentials', 'client_id': uid, 'client_secret': secret})
	r.raise_for_status()
	access_token = json.loads(r.text)['access_token']
	print(access_token)


	#### PROJ_PARSER.PY

	## Get information about projects

	url = 'https://api.intra.42.fr/v2/cursus/1/projects?access_token=%s' % (access_token)
	page = 1
	links = []
	while 1:
		
		# python3 implementation
		# f = urllib.request.urlopen(url + "&page=" + str(page))
		
		# python2 implementation
		f = urllib2.urlopen(url + "&page=" + str(page))
		
		res = json.loads(f.read())
		print(page)
		if res:
			links += res
		else:
			break
		page += 1
	print('Project info from API recieved')

	with open('42_projects_info.json', 'w') as outfile:
		json.dump(links, outfile)


	#### PROJ_BUILDER

	## Parse each project to save relevant information of names, ids and tiers

	# Load projects_info
	with open('42_projects_info.json', 'r') as file:
		js_proj = json.loads(file.read())

	# Manually list all exception cases
	exceptions = {'id': [118, 833, 48, 791, 62, 727, 394, 742, 370], 'score_type': ['sum', 'sum', 'sum', 'sum', 'sum', 'sum', 'sum', 'sum', 'sum']}

	projects_42 = {}

	# Filter only projects in Fremont and Paris (as there are many transfer students here)
	for item in js_proj:
		if any((el['id'] == 7 or el['id'] == 1) for el in item['campus']):
			
			if item['tier'] == None:
				projects_42[item['id']] = [item['slug'], -1]
			else:
				projects_42[item['id']] = [item['slug'], item['tier']]
			
			# Detect if parent project ordinary or one of the exception cases
			if not item['id'] in exceptions['id']:
				for child in item['children']:
					if item['tier'] == None:
						projects_42[child['id']] = [child['slug'], -1]
					else:
						projects_42[child['id']] = [child['slug'], item['tier']]
			else:
				# If the parent is in exception list than tier of each child should be calculated depending on it's category
				# print('EXCEPTION')
				i = exceptions['id'].index(item['id'])
				
				# If category is sum than all children sum up to tier of the parent
				if exceptions['score_type'][i] == 'sum':
					correction = math.log(len(item['children']),2)
					for child in item['children']:
						if item['tier'] == None:
							projects_42[child['id']] = [child['slug'], -1]
						else:
							projects_42[child['id']] = [child['slug'], item['tier'] - correction]
				else:
					for child in item['children']:
						projects_42[child['id']] = [child['slug'], -1]

	# pprint.pprint(projects_42)
	print('Projects data built')


	#### URL_PARSER.PY

	## Create Users URL list to parse through 42 API

	url = 'https://api.intra.42.fr/v2/campus/7/users?access_token=%s' % (access_token)
	page = 1
	links = []
	while 1:
	#for i in range(23, 29):

		# python3 implementation
		# f = urllib.request.urlopen(url + "&page=" + str(page))
		
		# python2 implementation
		f = urllib2.urlopen(url + "&page=" + str(page))

		res = json.loads(f.read())
		print(page)
		if res:
			links += res
		else:
			break
		page += 1
	df = pd.DataFrame(pd.read_json(json.dumps(links)))
	print('Users URL list from API built')


	#### GET_USER_INFO.PY

	## Parse user info by each url from users_url and save to a file

	people = []

	# Threding function to append users info to output list 'people'
	def recrod_info(user_url):
			
		# python3 implementation
		# f = urllib.request.urlopen(user_url+('?access_token=%s' % (access_token)))
		
		# python2 implementation
		f = urllib2.urlopen(user_url+('?access_token=%s' % (access_token)))
		
		tmp = json.loads(f.read())
		if type(tmp) == dict:
			people.append(tmp)
		else:
			print("Not a dict", user_url, type(tmp))


	# Multi-thread recording with external loop reducing number of threads to not overflood API with requests
	req_slower = 20
	length = len(df)
	for count in range(req_slower):
		threads = []
		print("Recording: %d%%" % int(count * 100 / req_slower))
		
		# Multi-threading to speed-up user info gathering from API
		for i in range((length * count) // req_slower, (length * (count + 1)) // req_slower):
			count += 1
			process = Thread(target=recrod_info, args=[df['url'][i]])
			process.start()
			threads.append(process)

		# Join is improtant before starting another process threading batch or world will go topsy turvy	
		for process in threads:
			process.join()
	print("Recording completed")

	# output as json
	with open('users_info.txt', 'w') as outfile:
		json.dump(people, outfile)

	print('User info saved')


	#### G_LEADERBOARD.PY

	## Create and update Leaderboards

	# Load user_info
	with open('users_info.txt', 'r') as file:
		js0 = json.loads(file.read())

	# Make a dataframe from initial json
	df0 = pd.DataFrame(js0)

	# Add levels and start_date to DaaFrame
	df0['level_42'] = -1.0
	df0['level_C'] = -1.0
	df0['start_date'] = ''
	df0['secs_start_date'] = -1.0
	df0['score_42'] = 0
	# df0['score_C'] = 0
	# df0['skills_42'] = ''
	# df0['skills_C'] = ''

	# From deeper json bring info (level, start_date) to top for Piscine_C and 42 courses
	id_42 = 1
	id_pisc_c = 4
	counter = 0
	t0 = datetime.datetime(2013, 9, 1)
	for item in js0:
		
		flag = 0
		if not item['staff?']:
		# For each el which user record levels and starting date
			for el in item['cursus_users']:
				if el['cursus_id'] == 4:
					df0.ix[counter, 'level_C'] = round(el['level'], 2)
					# df0.ix[counter, 'skills_C'] = [el['skills']]
				elif el['cursus_id'] == 1:
					df0.ix[counter, 'level_42'] = round(el['level'], 2)
					# df0.ix[counter, 'skills_42'] = el['skills']
					if el['begin_at'] != None and len(el['begin_at']) > 10:
						t = datetime.datetime.strptime(el['begin_at'][:10], '%Y-%m-%d')
						df0.ix[counter, 'start_date'] = el['begin_at'][:10]
						df0.ix[counter, 'secs_start_date'] = (t - t0).total_seconds()
					elif el['cursus']['created_at'] != None and len(el['cursus']['created_at']):
						# print('START DATE EXCEPTION', item['login'], el['cursus']['created_at'])
						t = datetime.datetime.strptime(el['cursus']['created_at'][:10], '%Y-%m-%d')
						df0.ix[counter, 'start_date'] = el['cursus']['created_at'][:10]
						df0.ix[counter, 'secs_start_date'] = (t - t0).total_seconds()
			
			# Record total score, sub scores for courses and scores for each project
			score_42 = 0
			# score_C = 0
			for projs in item['projects_users']:		
				if 1 in projs['cursus_ids'] and projs['status'] != 'parent':
					try:
						tier = projs['tier'] - 1
					except:
						try:
							tier = projects_42[projs['project']['id']][1] - 1
						except:
							tier = -1
					if projs['final_mark'] != None and projs['final_mark'] != 'null':
						score_42 += round(projs['final_mark'] * (2 ** tier), 0)
						# df0.ix[counter, projs['project']['slug']] = projs['final_mark']
				# elif 4 in projs['cursus_ids'] and projs['status'] != 'parent':
				# 	tier = projects_42[projs['project']['id']][1] - 1
				# 	score_C += projs['final_mark'] * (2 ** tier)
				# 	df0.ix[counter, projs['project']['slug']] = projs['final_mark']
			df0.ix[counter, 'score_42'] = score_42
			# df0.ix[counter, 'score_C'] = score_C
		counter += 1
	print('Students data built')

	# Preapre additional data fields for web deployment for Vincent
	df0 = df0[df0['secs_start_date'] > 0]
	df0['displayname'] = df0['last_name'] + ' ' + df0['first_name']
	df0['selected'] = 1
	df0['showing'] = 0
	# df0['total_score'] = df0['score_42'] + 
	
	select_lst = ['login', 'displayname', 'level_42', 'level_C', 'start_date', 'pool_month', 'pool_year', 'selected', 'showing']
	
	df0[select_lst].to_json('user_data.json', orient='records')

	# Update file with user data in google drive using PyDrive
	gauth = GoogleAuth()
	gauth.LocalWebserverAuth()
	drive = GoogleDrive(gauth)
	file_list = drive.ListFile({'q': "'0B1vi4ZYngkYQMzBWamRmemZOXzg' in parents and trashed=false"}).GetList()
	for file in file_list:
		if file['title'] == 'user_data.json':
			file.SetContentFile('user_data.json')
			file.Upload()
			print('%s File updated' % file['title'])
			break

	# Filter relevent data for Piscine_C and 42 courses
	df_C = df0[df0['level_C'] >= 0]
	# df_42 = df0[df0['pool_year'] >= '2016']
	df_42 = df0

	# Sort data and reindex
	df_42 = df_42.sort_values(['level_42', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
	df_C = df_C.sort_values(['level_C', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
	df_42['place'] = df_42.index + 1
	df_C['place'] = df_C.index + 1

	# Select only relevent rows to display/record for Tableau
	select_lst = ['place', 'login', 'last_name', 'first_name', 'level_42', 'start_date', 'pool_month', 'pool_year']
	df_42 = df_42[select_lst]
	df_C = df_C[['place', 'login', 'last_name', 'first_name', 'level_C', 'pool_month', 'pool_year']]

	# Print rank tables to terminal 
	# print(df_42)
	# print(df_C)

	# Output rank tables to csv
	# df_42.to_csv('leaderboard_42.csv')
	# df_C.to_csv('leaderboard_C.csv')

	# Connect to Google Spreadsheet
	scope =['https://spreadsheets.google.com/feeds']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('42stats-30ad16650adf.json', scope)
	gc = gspread.authorize(credentials)

	# Upadte 42 cursus
	sh = gc.open('leaderboard_42')
	worksheet = sh.get_worksheet(0)
	keys = list(df_42)
	cols = len(keys)
	rows = len(df_42['place'])
	header_list = worksheet.range(1, 1, 1, cols)
	for i in range(len(keys)):
		header_list[i].value = keys[i]
	worksheet.update_cells(header_list)
	cell_list = worksheet.range(2, 1, rows + 1, cols)
	for j in range(cols):
		print("Writing to 42 sheet: %d%%" % int(j * 100 / cols))
		for i in range(rows):
			cell_list[j + i * cols].value = df_42.iloc[i, j]

	# Update in batch
	print("Updating 42")
	worksheet.update_cells(cell_list)
	print("42 updated")

	# Upadte C cursus
	sh = gc.open('leaderboard_c')
	worksheet = sh.get_worksheet(0)
	keys = list(df_C)
	cols = len(keys)
	rows = len(df_C['place'])
	header_list = worksheet.range(1, 1, 1, cols)
	for i in range(len(keys)):
		header_list[i].value = keys[i]
	worksheet.update_cells(header_list)
	cell_list = worksheet.range(2, 1, rows + 1, cols)
	for j in range(cols):
		print("Writing to C Piscine sheet: %d%%" % int(j * 100 / cols))
		for i in range(rows):
			cell_list[j + i * cols].value = df_C.iloc[i, j]

	# Update in batch
	print("Updating C Piscine")
	worksheet.update_cells(cell_list)
	print("C Piscine updated")


	#### G_PORJ_USER.PY

	## Get information about users that completed certain projects and scores

	# Make a dataframe for info needed
	col_lst = ['login', 'project', 'score', 'exp_score']
	df0 = pd.DataFrame(columns=col_lst)

	# From deeper json bring info (level, start_date) to top for Piscine_C and 42 courses
	id_42 = 1
	id_pisc_c = 4
	counter = 0
	for item in js0:
		# Record total score, sub scores for courses and scores for each project
		for projs in item['projects_users']:
			if 1 in projs['cursus_ids'] and projs['status'] != 'parent':
				try:
					tier = projs['tier'] - 1
				except:
					try:
						tier = projects_42[projs['project']['id']][1] - 1
					except:
						tier = -1
				if projs['final_mark'] != None and projs['final_mark'] != 'null' and projs['final_mark'] > 0:
					df0.loc[counter] = [item['login'], projs['project']['slug'], projs['final_mark'], projs['final_mark'] * (2 ** tier)]
					counter += 1
				# else:
					# df0.loc[counter] = [item['login'], projs['project']['slug'], 0, 0]
	print('Collecting user info about projects completed')

	# Connect to Google Spreadsheet
	scope =['https://spreadsheets.google.com/feeds']
	credentials = ServiceAccountCredentials.from_json_keyfile_name('42stats-30ad16650adf.json', scope)
	gc = gspread.authorize(credentials)

	# Upadte 42 cursus
	sh = gc.open('data_42_course')
	worksheet = sh.get_worksheet(0)
	keys = list(df0)
	cols = len(keys)
	rows = len(df0)
	header_list = worksheet.range(1, 1, 1, cols)
	for i in range(len(keys)):
		header_list[i].value = keys[i]
		print(keys[i])
	worksheet.update_cells(header_list)
	cell_list = worksheet.range(2, 1, rows + 1, cols)
	for j in range(cols):
		print("Writing to sheet: %d%%" % int(j * 100 / cols))
		for i in range(rows):
			cell_list[j + i * cols].value = df0.iloc[i, j]

	# Update in batch
	print("Updating")
	worksheet.update_cells(cell_list)
	print("Scores and projects by users updated")

	print('Job completed')

# scheduled_job()
sched.start()