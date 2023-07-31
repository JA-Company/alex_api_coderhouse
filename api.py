import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

# Extract data form the API
api_url = os.getenv("API_URL")
querystring = {"league":"39","season":"2022","team":"33"} #It will be nice to let the user choose
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()

# Extract the relevant data from the response
league_info = data['response']['league']
# print(league_info)
team_info = data['response']['team']
# print(team_info)
fixtures = data['response']['fixtures']
# print(fixtures)
goals = data['response']['goals']
# print(goals)
biggest = data['response']['biggest']
# print(biggest)
clean_sheet = data['response']['clean_sheet']
# print(clean_sheet)
failed_to_score = data['response']['failed_to_score']
# print(failed_to_score)
penalty = data['response']['penalty']
# print(penalty)
lineups = data['response']['lineups']
print(lineups)
cards = data['response']['cards']
# print(cards)

# Create a list of dictionaries to represent the data in tabular format
# In the future will do this automatically with a for probably, I do not want to do it one by one
table_data = [
    # League Info
    {'Key': 'league_id', 'Value': league_info['id']},
    {'Key': 'league_name', 'Value': league_info['name']},
    {'Key': 'league_country', 'Value': league_info['country']},
    {'Key': 'league_season', 'Value': league_info['season']},
    {'Key': 'team_id', 'Value': team_info['id']},
    {'Key': 'team_name', 'Value': team_info['name']},
    # Fixtures
    {'Key': 'played_home', 'Value': fixtures['played']['home']},
    {'Key': 'played_away', 'Value': fixtures['played']['away']},
    {'Key': 'played_total', 'Value': fixtures['played']['total']},
    {'Key': 'wins_home', 'Value': fixtures['wins']['home']},
    {'Key': 'wins_away', 'Value': fixtures['wins']['away']},
    {'Key': 'wins_total', 'Value': fixtures['wins']['total']},
    {'Key': 'draws_home', 'Value': fixtures['draws']['home']},
    {'Key': 'draws_away', 'Value': fixtures['draws']['away']},
    {'Key': 'draws_total', 'Value': fixtures['draws']['total']},
    {'Key': 'loses_home', 'Value': fixtures['loses']['home']},
    {'Key': 'loses_away', 'Value': fixtures['loses']['away']},
    {'Key': 'loses_total', 'Value': fixtures['loses']['total']},
    # Goals
    {'Key': 'goals_home', 'Value': goals['for']['total']['home']},
    {'Key': 'goals_away', 'Value': goals['for']['total']['away']},
    {'Key': 'goals_total', 'Value': goals['for']['total']['total']},
    {'Key': 'goals_total', 'Value': goals['for']['minute']['0-15']['total']},
    {'Key': 'goals_total', 'Value': goals['for']['minute']['16-30']['total']},
    {'Key': 'goals_total', 'Value': goals['for']['minute']['31-45']['total']},
    {'Key': 'goals_total', 'Value': goals['for']['minute']['46-60']['total']},
    {'Key': 'goals_total', 'Value': goals['for']['minute']['61-75']['total']},
    {'Key': 'goals_total', 'Value': goals['for']['minute']['76-90']['total']},
]
# print(table_data)

# Create a DataFrame using pandas
# df = pandas.DataFrame(table_data)

# Print the DataFrame
# print(df)

# Redshift connection details
redshift_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

# Set up the Redshift connection
connection = psycopg2.connect(
    host = redshift_config['host'],
    dbname = redshift_config['dbname'], 
    user = redshift_config['user'],
    password = redshift_config['password'],
    port = redshift_config['port']
)

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

cursor.execute(create_table_query)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Table created and data inserted successfully.")