# Name: Jose Flores
# Assignment_7: Table Queries
# Date: 4/25/2020

""" import statements """
import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values

""" using our .env file """
secrets = dotenv_values(".env")

""" database config object """
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}

try:
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    print("\n-- DISPLAYING Studio RECORDS --")
    cursor.execute("SELECT * FROM studio")
    studios = cursor.fetchall()
    for studio in studios:
        print(f"Studio ID: {studio[0]}, Name: {studio[1]}")

    print("\n-- DISPLAYING Genre RECORDS --")
    cursor.execute("SELECT * FROM genre")
    genres = cursor.fetchall()
    for genre in genres:
        print(f"Genre ID: {genre[0]}, Name: {genre[1]}")

    print("\n-- DISPLAYING Short Film Names (Runtime < 120) --")
    cursor.execute("SELECT film_name, film_runtime FROM film WHERE film_runtime < 120")
    short_films = cursor.fetchall()
    for film in short_films:
        print(f"Film Name: {film[0]}, Runtime: {film[1]} minutes")

    print("\n-- DISPLAYING Films Grouped by Director --")
    cursor.execute("SELECT film_name, film_director FROM film ORDER BY film_director")
    films_by_director = cursor.fetchall()
    for film in films_by_director:
        print(f"Director: {film[1]}, Film: {film[0]}")

    input("\n\nPress any key to continue...")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")
    else:
        print(err)

finally:
    db.close()
