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
api_url = os.getenv("API_URL_LEAGUES") #https://api-football-v1.p.rapidapi.com/v3/leagues
querystring = {"season": "2023"}
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()

rows = []
for leagues_data in data['response']:
    league_info = leagues_data['league']
    country_info = leagues_data['country']
    season_info = leagues_data['seasons'][0]

    row = {
        'id': league_info['id'],
        'name': league_info['name'],
        'type': league_info['type'],
        'country': country_info['name'],
        'code': country_info['code'],
        'year': season_info['year'],
        'start_date': season_info['start'],
        'end_date': season_info['end'],
        'current': season_info['current'],
    }
    rows.append(row)
print(pandas.DataFrame(rows))

# # Set up the Redshift connection
connection = create_connection()
# Create a cursor to execute SQL queries
cursor = connection.cursor()
# Create Table in Redshift
table_name = 'leagues'
create_table_query = f'''
CREATE TABLE IF NOT EXISTS {table_name} (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    type VARCHAR(255),
    country VARCHAR(255),
    code VARCHAR(255),
    year INT,
    start_date DATE,
    end_date DATE,
    current BOOLEAN
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
        INSERT INTO {table_name} (id, name, type, country, code, year, start_date, end_date, current) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for row in rows:

            values = (
                row['id'], 
                row['name'], 
                row['type'], 
                row['country'], 
                row['code'], 
                row['year'], 
                row['start_date'], 
                row['end_date'], 
                row['current']
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