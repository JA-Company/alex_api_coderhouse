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
api_url = os.getenv("API_URL_LEAGUES") 
querystring = {"season": "2023", "id": "13"}
headers = {
	"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
	"X-RapidAPI-Host": os.getenv("X_RAPIDAPI_HOST")
}
response = requests.get(api_url, headers=headers, params=querystring)
data = response.json()
print(data)

rows = []
for leagues_data in data['response']:
    league_info = leagues_data['league']
    country_info = leagues_data['country']

    row = {
        'id': data['parameters']['id'],
        'name': league_info['name'],
        'type': league_info['type'],
        'country': country_info['name'],
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
    id int PRIMARY KEY,
    name varchar(255),
    type varchar(255),
    country varchar(255)
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
        INSERT INTO {table_name} (id, name, type, country)
        VALUES %s;
        '''
        values = [(row['id'], row['name'], row['type'], row['country']) for row in rows]
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"{len(rows)} records inserted successfully!")

except (Exception, psycopg2.DatabaseError) as error:
    print('Error:', error)

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()