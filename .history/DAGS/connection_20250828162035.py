import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# SOURCE DB config
source_db = {
    "host": os.getenv("SOURCE_HOST"),
    "user": os.getenv("SOURCE_USER"),
    "password": os.getenv("SOURCE_PASSWORD"),
    "database": os.getenv("SOURCE_DATABASE"),
}

# REPORTING DB config
reporting_db = {
    "host": os.getenv("REPORTING_HOST"),
    "user": os.getenv("REPORTING_USER"),
    "password": os.getenv("REPORTING_PASSWORD"),
    "database": os.getenv("REPORTING_DATABASE"),
}

# Helper function to connect
def get_connection(db_config):
    return mysql.connector.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
    )




# #connecting to database
# conn = mysql.connector.connect(
#     host=os.getenv("host"),         
#     user=os.getenv("user"),              
#     password=os.getenv("password"),              
# )

# #creating cursor object to execute queries
# cursor = conn.cursor()
