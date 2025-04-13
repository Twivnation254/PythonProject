import requests
import pandas as pd
import pyodbc
import logging

#setting up logging
logging.basicConfig(level=logging.INFO)

# Connect to MsSql
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=DataPipeline;'
    'Trusted_connection=yes;'
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Country')
BEGIN
    CREATE TABLE Country(
        Name NVARCHAR(50),
        Capital NVARCHAR(50),
        Region NVARCHAR(50),
        Population BIGINT
    )
END
""")
conn.commit()

 #Extracting data fro REST API
url = "https://restcountries.com/v3.1/all"
response = requests.get(url)

if response.status_code == 200:
     countries_data = response.json()
     logging.info("Fetched data from REST Countries API")
else:
    logging.error("API call failed!")
    exit()

# Transform to Dataframe

records = []
for country in countries_data:
    try:
        name = country.get('name', {}).get('common', '')
        capital = country.get('capital', [''])[0] if country.get('capital') else ''
        region = country.get('region', '')
        population = country.get('population', 0)
        records.append((name, capital, region, population))
    except Exception as e:
        logging.warning(f"Error parsing country: {e}")

df = pd.DataFrame(records, columns=['name', 'capital', 'region', 'population'])

# Loading into MsSql
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO Country (Name, Capital, Region, Population)
        VALUES (?, ?, ?, ?)
    """, row.name,row.capital, row.region, row.population)
conn.commit()

logging.info("Data successful loaded into MsSQL")