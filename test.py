import requests
import pyodbc
import pandas as pd
import logging
import json

logging.basicConfig(level=logging.INFO)

#connect to the MsSQL
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=Test;'
    'Trusted_connection=yes;'
)
cursor = conn.cursor()

#Creating table Product if doesn't exists

cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Product')
BEGIN
    CREATE TABLE Product(
        Id INT PRIMARY KEY,
        Title NVARCHAR(MAX),
        Price FLOAT,
        Description NVARCHAR(MAX),
        Category NVARCHAR(50),
        Image NVARCHAR(MAX)
    )
END
""")
conn.commit()

#Creating table Carts if doesn't exists

cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Carts')
BEGIN
    CREATE TABLE Carts(
        Id INT PRIMARY KEY,
        UserId INT,
        Products NVARCHAR(MAX),
        Date DATE
    )
END
""")
conn.commit()

#Creating table Users if doesn't exists

cursor.execute("""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Users')
BEGIN
  CREATE TABLE Users (
      Id INT PRIMARY KEY,
      Email NVARCHAR(100),
      Username NVARCHAR(100),
      City NVARCHAR(100)
  )
END
""")
conn.commit()


#Extracting from fake API
product = requests.get('https://fakestoreapi.com/products')
cart = requests.get('https://fakestoreapi.com/carts')
user = requests.get('https://fakestoreapi.com/users')

if product.status_code == 200:
     products_data = product.json()
     logging.info("Fetched data from FAKE Product API")
else:
    logging.error("API call failed!")
    exit()

if cart.status_code == 200:
     carts_data = cart.json()
     logging.info("Fetched data from FAKE Cart API")
else:
    logging.error("API call failed!")
    exit()

if user.status_code == 200:
     users_data = user.json()
     logging.info("Fetched data from FAKE Users API")
else:
    logging.error("API call failed!")
    exit()


#TRANSFORM PRODUCT DATA TO DATAFRAME
records1 = []
for products in products_data:
    try:
        id = products.get('id', 0)
        title = products.get('title', 0)
        price = products.get('price', 0)
        description = products.get('description', 0)
        category = products.get('category', 0)
        image = products.get('image', 0)
        records1.append((id, title, price, description, category, image))
    except Exception as e:
        logging.warning(f"Error parsing country: {e}")

df_products = pd.DataFrame(records1, columns=['id', 'title', 'price', 'description', 'category', 'image'])

#TRANSFORM CART DATA TO DATAFRAME
records2 = []
for carts in carts_data:
    try:
        id = carts.get('id', 0)
        userId = carts.get('userId', 0)
        products = json.dumps(carts.get('products', []))
        date = carts.get('date', 0)
        records2.append((id, userId, products, date))
    except Exception as e:
        logging.warning(f"Error parsing country: {e}")

df_carts = pd.DataFrame(records2, columns=['id', 'userId', 'products', 'date'])

#TRANSFORM USER DATA TO DATAFRAME
records3 = []
for users in users_data:
    try:
        id = users.get('id', 0)
        email = users.get('email', 0)
        username = users.get('username', 0)
        city = users.get('address', {}).get('city', '')
        records3.append((id, email, username, city))
    except Exception as e:
        logging.warning(f"Error parsing country: {e}")

df_users = pd.DataFrame(records3, columns=['id', 'email', 'username', 'city'])

# Loading into MsSql
for _, row in df_products.iterrows():
    cursor.execute("""
        INSERT INTO Product (Id, Title, Price, Description, Category, Image)
        VALUES (?, ?, ?, ?, ?, ?)
    """, row.id, row.title, row.price, row.description, row.category, row.image)
conn.commit()

for _, row in df_carts.iterrows():
    cursor.execute("""
        INSERT INTO Carts (Id, UserId, Products, Date)
        VALUES (?, ?, ?, ?)
    """, row.id, row.userId, row.products, row.date)
conn.commit()

for _, row in df_users.iterrows():
    cursor.execute("""
        INSERT INTO Users (Id, Email, Username, City)
        VALUES (?, ?, ?, ?)
    """, row.id, row.email, row.username, row.city)
conn.commit()
logging.info("Data successful loaded into MsSQL")