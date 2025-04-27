# Jose Flores
# 4/27/2025
# Assignment_ 8: Movies, Update & Delete

import mysql.connector
from mysql.connector import errorcode
from dotenv import dotenv_values

# Load environment variables from .env
secrets = dotenv_values(".env")

# Database config
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}

# Function to display all films in custom order
def show_films(cursor, title):
    query = """
        SELECT film_name AS Name, film_director AS Director,
               genre_name AS Genre, studio_name AS Studio
        FROM film
        INNER JOIN genre ON film.genre_id = genre.genre_id
        INNER JOIN studio ON film.studio_id = studio.studio_id
        ORDER BY FIELD(film_name, 'Gladiator', 'Alien', 'Get Out', 'Star Wars'), film_name
    """

    cursor.execute(query)
    films = cursor.fetchall()

    print(f"\n-- {title} --")
    for film in films:
        print(f"Film Name: {film[0]}")
        print(f"Director: {film[1]}")
        print(f"Genre Name ID: {film[2]}")
        print(f"Studio Name: {film[3]}")
        print()

# Main script
try:
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    # STEP 0: Clean up duplicates
    for title in ["Star Wars", "Inception", "Gladiator", "Get Out", "Alien"]:
        cursor.execute("DELETE FROM film WHERE film_name = %s", (title,))
    db.commit()

    # STEP 1: Insert Alien as SciFi
    cursor.execute("SELECT COUNT(*) FROM film WHERE film_name = 'Alien'")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ("Alien", "1979", 117, "Ridley Scott", 1, 2))  # 20th Century Fox, SciFi
        db.commit()

    # STEP 2: Insert Get Out as Horror
    cursor.execute("SELECT COUNT(*) FROM film WHERE film_name = 'Get Out'")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ("Get Out", "2017", 104, "Jordan Peele", 2, 1))  # Blumhouse, Horror
        db.commit()

    # STEP 3: Insert Gladiator as Drama
    cursor.execute("SELECT COUNT(*) FROM film WHERE film_name = 'Gladiator'")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ("Gladiator", "2000", 155, "Ridley Scott", 3, 3))  # Universal, Drama
        db.commit()

    # STEP 4: Show initial films (Gladiator first)
    show_films(cursor, "DISPLAYING FILMS")

    # STEP 5: Insert Star Wars as SciFi
    cursor.execute("SELECT COUNT(*) FROM film WHERE film_name = 'Star Wars'")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ("Star Wars", "1977", 121, "George Lucas", 1, 2))  # 20th Century Fox, SciFi
        db.commit()

    # STEP 6: Show after Star Wars insert
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    # STEP 7: Update Alien's genre to Horror
    cursor.execute("UPDATE film SET genre_id = 1 WHERE film_name = 'Alien'")  # Horror = 1
    db.commit()

    # STEP 8: Show after update
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE - Changed Alien to Horror")

    # STEP 9: Delete Gladiator
    cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator'")
    db.commit()

    # STEP 10: Show after delete
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("The supplied username or password are invalid.")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("The specified database does not exist.")
    else:
        print(err)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'db' in locals() and db.is_connected():
        db.close()
