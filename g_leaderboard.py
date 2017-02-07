#import plotly.plotly as py
#from plotly.tools import FigureFactory as FF
import json
import pandas as pd
from collections import Counter
import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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
df_42.to_csv('leaderboard_42.csv')
df_C.to_csv('leaderboard_C.csv')
df_42.to_json('leaderboard_42.json', orient='records')

#print 42 rank table to terminal 
#print(df_42[['place', 'login', 'last_name', 'first_name', 'level_42', 'start_date', 'pool_month','pool_year']])

#print C piscine rank table to terminal
#print(df_C[['place', 'login', 'last_name', 'first_name', 'level_C', 'pool_month','pool_year']])

#upload to plotly (results are not visually good though)
#table = FF.create_table(df_42[['place', 'user', 'last_name', 'first_name', 'level_42', 'pool_month','pool_year']])
#py.iplot(table, filename='42 US Leaderboard')

#Connect to Google Spreadsheet
scope =['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('42stats-30ad16650adf.json', scope)
gc = gspread.authorize(credentials)

#worksheet.update_acell('B1', 'Bingo!')

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





