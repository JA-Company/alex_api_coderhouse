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
api_url = os.getenv("API_URL_LIBERTADORES_TEAM") #https://api-football-v1.p.rapidapi.com/v3/teams/statistics
querystring = {"season": "2023", "league": "13"} #I am planning to be more dynamic here, let the user choose the team/season/league.
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()
print(data)

rows = []
for team_data in data['response']:
    team_info = team_data['team']
    venue_info = team_data['venue']

    row = {
        'league': data['parameters']['league'],
        'season': data['parameters']['season'],
        'team_id': team_info['id'],
        'team_name': team_info['name'],
        'team_code': team_info['code'],
        'team_country': team_info['country'],
        'team_founded': team_info['founded'],
        'team_logo_image': team_info['logo'],
        'stadium_id': venue_info['id'],
        'stadium_name': venue_info['name'],
        'stadium_address': venue_info['address'],
        'stadium_city': venue_info['city'],
        'stadium_capacity': venue_info['capacity'],
        'stadium_surface': venue_info['surface'],
        'stadium_image': venue_info['image'],
    }
    rows.append(row)
print(pandas.DataFrame(rows))

# Extract the relevant data
# data_extracted = {
#     'parameters': data['parameters'],
#     'league_info': data['response']['league'],
#     'team_info': data['response']['team'],
#     'fixtures': data['response']['fixtures'],
#     'goals': data['response']['goals'],
#     'biggest': data['response']['biggest'],
#     'clean_sheet': data['response']['clean_sheet'],
#     'failed_to_score': data['response']['failed_to_score'],
#     'penalty': data['response']['penalty'],
#     'lineups': data['response']['lineups'],
#     'cards': data['response']['cards'],
# }

# for key, value in data_extracted.items():
#     print(value)

# print(data_extracted['league_info'])
# print(data_extracted['fixtures'])

# print(data_extracted['fixtures'])

# Returns the list of dictionaries with 'Key' and 'Value' pairs for the dictionary
# def create_list_of_dicts(data):
#     if isinstance(data, dict):
#         return [{'Key': key, 'Value': value} for key, value in data.items()]
#     elif isinstance(data, list):
#         result = []
#         for item in data:
#             for key, value in item.items():
#                 result.append({'Key': key, 'Value': value})
#         return result
#     else:
#         return []


# # Create DataFrames for each value in the data_extracted dictionary
# # dataframes = {}
# # for key, value in data_extracted.items():
# #     dataframes[key] = pandas.DataFrame([value])
# #     print(dataframes[key])
# #     print("\n")

# # # Now you can access each DataFrame using its corresponding key
# # print(dataframes['parameters'])  # DataFrame for 'parameters'
# # print(dataframes['league_info'])  # DataFrame for 'league_info'
# # print(dataframes['team_info'])  # DataFrame for 'team_info'

# # # Prepareing all the Lists so we can insert keys and values into columns in a table
# # parameters_list = create_list_of_dicts(data_extracted['parameters'])
# # print("\nParameter List:")
# # print(parameters_list)

# # testing = [data_extracted['parameters']]
# # parameter_df = pandas.DataFrame(testing)
# # print(parameter_df)

# # league_info_list = create_list_of_dicts(data_extracted['league_info'])
# # league_info_list_testing = [data_extracted['league_info']]

# # print("\nLeague Info List: ")
# # print(league_info_list)

# team_info_list = create_list_of_dicts(data_extracted['team_info'])
# # print("\nTeam Info List: ")
# # print(team_info_list)

# fixtures_list = create_list_of_dicts(data_extracted['fixtures'])
# # print("\nFixtures List: ")
# # print(fixtures_list)

# goals_list = create_list_of_dicts(data_extracted['goals'])
# # print("\nGoals List: ")
# # print(goals_list)

# biggest_list = create_list_of_dicts(data_extracted['biggest'])
# # print("\nBiggest List: ")
# # print(biggest_list)

# clean_sheet_list = create_list_of_dicts(data_extracted['clean_sheet'])
# # print("\nClean Sheet List: ")
# # print(clean_sheet_list)

# failed_to_score_list = create_list_of_dicts(data_extracted['failed_to_score'])
# # print("\nFailed to Score List: ")
# # print(failed_to_score_list)

# penalty_list = create_list_of_dicts(data_extracted['penalty'])
# # print("\nPenalties: ")
# # print(penalty_list)

# lineups_list = create_list_of_dicts(data_extracted['lineups'])
# # print("\nLineups: ")
# # print(lineups_list)

# cards_list = create_list_of_dicts(data_extracted['cards'])
# # print("\nCards List: ")
# # print(cards_list)
# # print("\n \n")

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