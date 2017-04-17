# 42stats

Purpose:
Representation and visualization of users stats (US campus):
1) US leaderboard for 42 course
2) US leaderboard for Piscine C
3) Projects by US students and scores received

Implementation:
Python script on Heroku with advanced python scheduler (for auto updates) 
1) Gathering data from 42 API
2) Parsing, cleaning, restructuring data for output tables
3) Saving data via google API to Google Sheets (for Tableau) and Google Drive (for Web application)
4a) Tableau workbook connected to Google Sheet and constructed Dashboards
4b) Web app using info from Google Drive to present (made by Vincent Portell)

Structure:
hk_leaderboard folder: folder with all scripts gathered in clock.py and files to run on Heroku (credentials from Heroku need to be included).
root directory scripts:
- parser.py: get users URL from 42 API
- proj_parser.py: get project info from 42 API
- get_user_info.py: for each user URL get full user's info from 42 API (milti-threded with req_slower not to overload server)
- g_leaderboard.py: main script that cleans, restructures, and saves data for leaderboards to google drive and google sheets
- g_project_users.py: the main script that cleans, restructures, and save data for projects done by users chart to google sheet

42 US Leaderboard:
https://public.tableau.com/profile/mikhail.filipchuk#!/vizhome/42USLeaderboard/Dashboard1

Piscine C US Leaderboard:
https://public.tableau.com/profile/mikhail.filipchuk#!/vizhome/PiscineCLeaderboard/Dashboard1

42 Course Projects and Scores y users:
https://public.tableau.com/profile/mikhail.filipchuk#!/vizhome/42CourseProjectsandScoresbyusers/Dashboard1
