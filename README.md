# 1337 Leaderboards

Purpose:
Representation and visualization of users stats (1337 Benguerir Campus):
1) Moroccan leaderboard for 42 cursus
2) Moroccan leaderboard for Piscine C

Implementation:
Python script on Heroku with advanced python scheduler (for auto updates) 
1) Gathering data from 42 API
2) Parsing, cleaning, restructuring data for output tables
3) Saving data via google API to Google Sheets (for Tableau) and Google Drive (for Web application)
4a) Tableau workbook connected to Google Sheet and constructed Dashboards
4b) Web app using info from Google Drive to present

Structure:
- parser.py: get users URL from 42 API
- proj_parser.py: get project info from 42 API
- get_user_info.py: for each user URL get full user's info from 42 API (milti-threded with req_slower not to overload server)
- g_leaderboard.py: main script that cleans, restructures, and saves data for leaderboards to google drive and google sheets
- g_project_users.py: the main script that cleans, restructures, and save data for projects done by users chart to google sheet


