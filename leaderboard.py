import plotly.plotly as py
from plotly.tools import FigureFactory as FF
import json
import pandas as pd

with open('users_info.txt', 'r') as file:
	js0 = json.loads(file.read())
level = []
for item in js0:
	try:
		level.append(round(item['cursus_users'][0]['level'], 2))
	except:
		level.append(0)
df0 = pd.DataFrame(js0)
df0.loc[:,'level'] = pd.Series(level, index=df0.index)
df0 = df0.sort_values(['level', 'last_name'], ascending=[0, 1]).reset_index(drop=True)
#print(list(df0))
#'<a href="https://profile.intra.42.fr/users/%s">%s</a>' % (login, login)
df0['user'] = '<a href="https://profile.intra.42.fr/users/' + df0['login'] + '">' + df0['login'] + '</a>' 
df0['place'] = df0.index + 1
print(df0[['place', 'login', 'last_name', 'first_name', 'level']])
#table = FF.create_table(df0[['place', 'user', 'first_name', 'last_name', 'level']])
#py.iplot(table, filename='simple_table')
