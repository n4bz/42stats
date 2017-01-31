import plotly.plotly as py
from plotly.tools import FigureFactory as FF
from plotly.graph_objs import *
import json
import pandas as pd
from pivottablejs import pivot_ui

with open('users_info.txt', 'r') as file:
	js0 = json.loads(file.read())
id_42 = 1
id_pisc_c = 4
level_42 = []
level_pisc_c = []
for item in js0:
	try:
		level_pisc_c.append(*[round(el['level'], 2) for el in item['cursus_users'] if el['cursus_id'] == 4])
		try:
			level_42.append(*[round(el['level'], 2) for el in item['cursus_users'] if el['cursus_id'] == 1])
		except:
			level_42.append(0)
	except:
		level_pisc_c.append(0)
		level_42.append(0)
df0 = pd.DataFrame(js0)
df0.loc[:,'level_42'] = pd.Series(level_42, index=df0.index)
df0.loc[:,'level_C'] = pd.Series(level_pisc_c, index=df0.index)
df0['user'] = '<a href="https://profile.intra.42.fr/users/' + df0['login'] + '">' + df0['login'] + '</a>' 
#df0.loc[:,'cursus'] = pd.Series(cursus, index=df0.index)
df0 = df0[df0['pool_year'] >= '2016']
df_42 = df0.sort_values(['level_42', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
df_C = df0.sort_values(['level_C', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
#print(list(df0))
#'<a href="https://profile.intra.42.fr/users/%s">%s</a>' % (login, login)
df_42['place'] = df_42.index + 1
print(df_42[['place', 'login', 'last_name', 'first_name', 'level_42', 'pool_month','pool_year']])
df_C['place'] = df_C.index + 1
print(df_C[['place', 'login', 'last_name', 'first_name', 'level_C', 'pool_month','pool_year']])
#table = FF.create_table(df_42[['place', 'user', 'last_name', 'first_name', 'level_42', 'pool_month','pool_year']])
#py.iplot(table, filename='42 US Leaderboard')