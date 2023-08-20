# Considerations: 
    # The API contains different information from the Manchester United on the season 2022. 
    # The idea is to get historical information for all the years, and everytime Manchester UTD plays new info will appear. 
    # I will have many tables and join them on keys. Right now I only crated 1 table, this is the first deliverable .

import requests
import psycopg2
import os
from dotenv import load_dotenv
from database import create_connection
import pandas 
import datetime

load_dotenv()  # take environment variables from .env.

current_date = datetime.date.today()

# Extract data form the API
api_url = os.getenv("API_URL_FIXTURES") #https://api-football-v1.p.rapidapi.com/v3/fixtures
querystring = {"season": "2023", "date": current_date}
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()
# print(data)

rows = []
for fixture_data in data['response']:
    fixture_info = fixture_data['fixture']
    fixture_league = fixture_data['league']
    fixture_teams = fixture_data['teams']
    fixture_goals = fixture_data['goals']

    row = {
        'id': fixture_info['id'],
        'referee': fixture_info['referee'],
        'added_at': fixture_info['timestamp'],
        'stadium_name': fixture_info['venue']['name'],
        'stadium_id': fixture_info['venue']['id'],
        'stadium_city': fixture_info['venue']['city'], 
        'status_long': fixture_info['status']['long'], 
        'status_short': fixture_info['status']['short'], 
        'elapsed': fixture_info['status']['elapsed'], 
        'league_id': fixture_league['id'],
        'league_season': fixture_league['season'],
        'team_home_id': fixture_teams['home']['id'],
        'team_home_name': fixture_teams['home']['name'],
        'team_away_id': fixture_teams['away']['id'],
        'team_away_name': fixture_teams['away']['name'],
        'goals_home': fixture_goals['home'],
        'goals_away': fixture_goals['away'],
        'loaded_date': current_date
    }
    rows.append(row)
print(pandas.DataFrame(rows))

# # Set up the Redshift connection
connection = create_connection()
# Create a cursor to execute SQL queries
cursor = connection.cursor()
# Create Table in Redshift
table_name = 'fixtures'
create_table_query = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY,
    referee VARCHAR(255),
    added_at INT,
    stadium_name VARCHAR(255),
    stadium_id INT,
    stadium_city VARCHAR(255),
    status_long VARCHAR(255),
    status_short VARCHAR(255),
    elapsed INT,
    league_id INT, 
    league_season INT,
    team_home_id INT,
    team_home_name VARCHAR(255),
    team_away_id INT,
    team_away_name VARCHAR(255),
    goals_home INT,
    goals_away INT
);
'''
try:
    # Check if the table exists
    check_table_query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()

    if table_exists:
        print("Hey, the table is already created! No need to re-create it.")
    else:
        # Execute the create table query
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table {table_name} created successfully!")

    # Insert data into the table
    if rows:
        insert_query = f'''
        INSERT INTO {table_name} (id, referee, added_at, stadium_name, stadium_id, stadium_city, status_long, status_short, elapsed, league_id, league_season, team_home_id, team_home_name, team_away_id, team_away_name, goals_home, goals_away) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for row in rows:

            values = (
                row['id'], 
                row['referee'], 
                row['added_at'], 
                row['stadium_name'], 
                row['stadium_id'], 
                row['stadium_city'], 
                row['status_long'], 
                row['status_short'], 
                row['elapsed'],
                row['league_id'], 
                row['league_season'], 
                row['team_home_id'], 
                row['team_home_name'], 
                row['team_away_id'], 
                row['team_away_name'], 
                row['goals_home'], 
                row['goals_away']
            )
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"{len(rows)} records inserted successfully!")

except (Exception, psycopg2.DatabaseError) as error:
    print('Error:', error)

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()