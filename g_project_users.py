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
			print('EXCEPTION')
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

pprint.pprint(projects_42)

# Load user_info
with open('users_info.txt', 'r') as file:
	js0 = json.loads(file.read())

# Make a dataframe from initial json
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
				# df.loc[counter] = [item['login'], projs['project']['slug'], 0, 0]

# Output 42 rank table to csv
# df0.to_json('leaderboard_42.json', orient='records')

# Print rank tables to terminal 
print(df0)

# Output rank tables to csv
# df0.to_csv('leaderboard_42.csv')


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

Update in batch
print("Updating")
worksheet.update_cells(cell_list)
print("Process completed")