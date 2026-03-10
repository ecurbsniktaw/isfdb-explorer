"""
Shared database connection for ISFDB report scripts.
Edit the CONFIG dict below to match your local MySQL setup.
"""
import mysql.connector

CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Bpw591968$",
    "database": "isfdb",
}


def get_connection():
    return mysql.connector.connect(**CONFIG)
