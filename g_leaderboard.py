import json
import pandas as pd
import datetime
import time
import math
import pprint
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# Load projects_info
with open('42_projects_info.json', 'r') as file:
	js0 = json.loads(file.read())

# Manually list all exception cases
exceptions = {'id': [118, 833, 48, 791, 62, 727, 394, 742, 370], 'score_type': ['sum', 'sum', 'sum', 'sum', 'sum', 'sum', 'sum', 'sum', 'sum']}

projects_42 = {}

# Filter only projects in Fremont and Paris (as there are many transfer students here)
for item in js0:
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
print('Projects data build')

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
				#df0.ix[counter, 'skills_C'] = [el['skills']]
			elif el['cursus_id'] == 1:
				df0.ix[counter, 'level_42'] = round(el['level'], 2)
				#df0.ix[counter, 'skills_42'] = el['skills']
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
		#score_C = 0
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
					#df0.ix[counter, projs['project']['slug']] = projs['final_mark']
			# elif 4 in projs['cursus_ids'] and projs['status'] != 'parent':
			# 	tier = projects_42[projs['project']['id']][1] - 1
			# 	score_C += projs['final_mark'] * (2 ** tier)
			# 	df0.ix[counter, projs['project']['slug']] = projs['final_mark']
		df0.ix[counter, 'score_42'] = score_42
		#df0.ix[counter, 'score_C'] = score_C
	counter += 1
print('Students data build')

# Preapre additional data fields for web deployment for Vincent
df0['displayname'] = df0['last_name'] + ' ' + df0['first_name']
df0['selected'] = 1
df0['showing'] = 0
# df0['total_score'] = df0['score_42'] + 
select_lst = ['login', 'displayname', 'level_42', 'level_C', 'start_date', 'pool_month', 'pool_year', 'selected', 'showing']
df_vinc = df0[df0['secs_start_date'] > 0]
df_vinc[select_lst].to_json('user_data.json', orient='records')

# Filter relevent data for Piscine_C and 42 courses
# df0 = df0[df0['pool_year'] >= '2016']
df_42 = df0[df0['secs_start_date'] > 0]
df_C = df0[df0['level_C'] >= 0]

# Sort data and reindex
df_42 = df_42.sort_values(['level_42', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
df_C = df_C.sort_values(['level_C', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
df_42['place'] = df_42.index + 1
df_C['place'] = df_C.index + 1


# Select data to send to Vincent's web
# df_42 = df_42[['place', 'login', 'last_name', 'first_name', 'level_42', 'start_date', 'pool_month', 'pool_year', 'selected', 'showing']]
# df_C = df_C[['place', 'login', 'last_name', 'first_name', 'level_C', 'pool_month', 'pool_year', 'selected', 'showing']]

# Output 42 rank table to csv
# df_42.to_json('leaderboard_42.json', orient='records')
# df_C.to_json('leaderboard_C.json', orient='records')


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
print("Process completed")