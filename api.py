# Considerations: 
    # The API contains different information from the Manchester United on the season 2022. 
    # The idea is to get historical information for all the years, and everytime Manchester UTD plays new info will appear. 
    # I will have many tables and join them on keys. Right now I only crated 1 table, this is the first deliverable .

import requests
import psycopg2
import os
from dotenv import load_dotenv
from database import create_connection

load_dotenv()  # take environment variables from .env.

# Extract data form the API
api_url = os.getenv("API_URL") #https://api-football-v1.p.rapidapi.com/v3/teams/statistics
querystring = {"league":"39","season":"2022","team":"33"} #I am planning to be more dynamic here, let the user choose the team/season/league.
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()

# Extract the relevant data from the response
data_extracted = {
    'parameters': data['parameters'],
    'league_info': data['response']['league'],
    'team_info': data['response']['team'],
    'fixtures': data['response']['fixtures'],
    'goals': data['response']['goals'],
    'biggest': data['response']['biggest'],
    'clean_sheet': data['response']['clean_sheet'],
    'failed_to_score': data['response']['failed_to_score'],
    'penalty': data['response']['penalty'],
    'lineups': data['response']['lineups'],
    'cards': data['response']['cards'],
}

# Returns the list of dictionaries with 'Key' and 'Value' pairs for the dictionary
def create_list_of_dicts(data):
    if isinstance(data, dict):
        return [{'Key': key, 'Value': value} for key, value in data.items()]
    elif isinstance(data, list):
        result = []
        for item in data:
            for key, value in item.items():
                result.append({'Key': key, 'Value': value})
        return result
    else:
        return []

# Prepareing all the Lists so we can insert keys and values into columns in a table
parameters_list = create_list_of_dicts(data_extracted['parameters'])
print("\nParameter List:")
print(parameters_list)

league_info_list = create_list_of_dicts(data_extracted['league_info'])
print("\nLeague Info List: ")
print(league_info_list)

team_info_list = create_list_of_dicts(data_extracted['team_info'])
print("\nTeam Info List: ")
print(team_info_list)

fixtures_list = create_list_of_dicts(data_extracted['fixtures'])
print("\nFixtures List: ")
print(fixtures_list)

goals_list = create_list_of_dicts(data_extracted['goals'])
print("\nGoals List: ")
print(goals_list)

biggest_list = create_list_of_dicts(data_extracted['biggest'])
print("\nBiggest List: ")
print(biggest_list)

clean_sheet_list = create_list_of_dicts(data_extracted['clean_sheet'])
print("\nClean Sheet List: ")
print(clean_sheet_list)

failed_to_score_list = create_list_of_dicts(data_extracted['failed_to_score'])
print("\nFailed to Score List: ")
print(failed_to_score_list)

penalty_list = create_list_of_dicts(data_extracted['penalty'])
print("\nPenalties: ")
print(penalty_list)

lineups_list = create_list_of_dicts(data_extracted['lineups'])
print("\nLineups: ")
print(lineups_list)

cards_list = create_list_of_dicts(data_extracted['cards'])
print("\nCards List: ")
print(cards_list)
print("\n \n")

# Set up the Redshift connection
connection = create_connection()
# Create a cursor to execute SQL queries
cursor = connection.cursor()
# Create Table in Redshift
table_name = 'league_info'
create_table_query = f'''
CREATE TABLE {table_name} (
    league_id int,
    league_name varchar(255),
    league_country varchar(255),
    league_season varchar(255),
    team_id int,
    team_name varchar(255)
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

except (Exception, psycopg2.DatabaseError) as error:
    print('Error:', error)

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()