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

# Filter only projects in Benguerir and Khourbiga 
for item in js0:
	if any((el['id'] == 21 or el['id'] == 16) for el in item['campus']):
		
		projects_42[item['id']] = [item['slug']]


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
id_42 = 21
id_pisc_c = 6
counter = 0
for item in js0:
	
	flag = 0
	if not item['staff?']:
	# For each el which user record levels and starting date
		for el in item['cursus_users']:
			if el['cursus_id'] == 6:
				df0.loc[counter, 'level_C'] = round(el['level'], 2)
				#df0.loc[counter, 'skills_C'] = [el['skills']]
			elif el['cursus_id'] == 21:
				df0.loc[counter, 'level_42'] = round(el['level'], 2)
				#df0.loc[counter, 'skills_42'] = el['skills']
				#if el['begin_at'] != None and len(el['begin_at']) > 10:
				#	t = datetime.datetime.strptime(el['begin_at'][:10], '%Y-%m-%d')
				#	df0.loc[counter, 'start_date'] = el['begin_at'][:10]
				#	df0.loc[counter, 'secs_start_date'] = (t - t0).total_seconds()
				#elif el['cursus']['created_at'] != None and len(el['cursus']['created_at']):
					# print('START DATE EXCEPTION', item['login'], el['cursus']['created_at'])
				#	t = datetime.datetime.strptime(el['cursus']['created_at'][:10], '%Y-%m-%d')
				#	df0.loc[counter, 'start_date'] = el['cursus']['created_at'][:10]
				#	df0.loc[counter, 'secs_start_date'] = (t - t0).total_seconds()
		
		# Record total score, sub scores for courses and scores for each project
		score_42 = 0
		#score_C = 0
		for projs in item['projects_users']:
			if 21 in projs['cursus_ids'] and projs['status'] != 'parent':		
				if projs['final_mark'] != None and projs['final_mark'] != 'null':
					score_42 += round(projs['final_mark'], 0)
					#df0.loc[counter, projs['project']['slug']] = projs['final_mark']
			# elif 4 in projs['cursus_ids'] and projs['status'] != 'parent':
			# 	tier = projects_42[projs['project']['id']][1] - 1
			# 	score_C += projs['final_mark'] * (2 ** tier)
			# 	df0.loc[counter, projs['project']['slug']] = projs['final_mark']
		df0.loc[counter, 'score_42'] = score_42
		#df0.loc[counter, 'score_C'] = score_C
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
df_42 = df0[df0['level_42'] >= 0]
df_C = df0[df0['level_C'] >= 0]

# Sort data and reindex
df_42 = df_42.sort_values(['level_42', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
df_C = df_C.sort_values(['level_C', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
df_42['place'] = df_42.index + 1
df_C['place'] = df_C.index + 1


# Output 42 rank table to json
# df_42.to_json('leaderboard_42.json', orient='records')
# df_C.to_json('leaderboard_C.json', orient='records')


# Select only relevent rows to display/record for Tableau
select_lst = ['place', 'login', 'last_name', 'first_name', 'level_42', 'pool_month', 'pool_year']
df_42 = df_42[select_lst]
df_C = df_C[['place', 'login', 'last_name', 'first_name', 'level_C', 'pool_month', 'pool_year']]


# Print rank tables to terminal 
# print(df_42)
# print(df_C)

# Output rank tables to csv
df_42.to_csv('leaderboard_42.csv')
df_C.to_csv('leaderboard_C.csv')


# Connect to Google Spreadsheet
scope =['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('rankings-1609572152705-e1fcb9657f3b.json', scope)
gc = gspread.authorize(credentials)

# Upadte 42 cursus
print("Updating 1337 rankings")

sh = gc.open('1337_Rankings')
with open('leaderboard_42.csv', 'r') as file_obj:
    content = file_obj.read()
    gc.import_csv(sh.id, data=content)

print("1337 rankings updated")

# Update C cursus
print("Updating C Piscine")

sh = gc.open('leaderboard_c')
with open('leaderboard_C.csv', 'r') as file_obj:
    content = file_obj.read()
    gc.import_csv(sh.id, data=content)

print("Process completed")