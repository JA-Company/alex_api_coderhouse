# I AM STILL WORKING ON IT, THIS IS ONE OF THE MAIN TABLES

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

load_dotenv()  # take environment variables from .env.

# Extract data form the API
api_url = os.getenv("API_URL_TEAMS") #https://api-football-v1.p.rapidapi.com/v3/teams/statistics
querystring = {"season": "2023", "league": "13"} #I am planning to be more dynamic here, let the user choose the team/season/league.
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()
# print(data)


rows = []
for team_data in data['response']:
    team_info = team_data['team']
    venue_info = team_data['venue']

    row = {
        'league_id': data['parameters']['league'],
        'season': data['parameters']['season'],
        'team_id': team_info['id'],
        'team_name': team_info['name'],
        'team_code': team_info['code'],
        'team_country': team_info['country'],
        'team_founded': team_info['founded'],
        'stadium_id': venue_info['id'],
        'stadium_name': venue_info['name'],
        'stadium_address': venue_info['address'],
        'stadium_city': venue_info['city']
    }
    rows.append(row)
print(pandas.DataFrame(rows))



# Set up the Redshift connection
connection = create_connection()
# Create a cursor to execute SQL queries
cursor = connection.cursor()
# Create Table in Redshift
table_name = 'teams'
create_table_query = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    league_id int,
    season varchar(255),
    team_id int PRIMARY KEY,
    team_name varchar(255),
    team_code varchar(255),
    team_country varchar(255),
    team_founded int,
    stadium_id int,
    stadium_name varchar(255),
    stadium_address varchar(255),
    stadium_city varchar(255)
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
    # Insert data into the table
    if rows:
        insert_query = f'''
        INSERT INTO {table_name} (league_id, season, team_id, team_name, team_code, team_country, team_founded, stadium_id, stadium_name, stadium_address, stadium_city)
        VALUES (%(league_id)s, %(season)s, %(team_id)s, %(team_name)s, %(team_code)s, %(team_country)s, %(team_founded)s, %(stadium_id)s, %(stadium_name)s, %(stadium_address)s, %(stadium_city)s);
        '''
        cursor.executemany(insert_query, rows)
        connection.commit()
        print(f"{len(rows)} records inserted successfully!")

except (Exception, psycopg2.DatabaseError) as error:
    print('Error:', error)

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()