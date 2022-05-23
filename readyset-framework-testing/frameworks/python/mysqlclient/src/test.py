import os
import time
import timeit
import urllib.parse

import MySQLdb

args = {}

args["host"] = os.environ.get("RS_HOST")
if args["host"] is None:
    print("RS_HOST must be set")
    os.Exit(1)

port = os.environ.get("RS_PORT")
if port is None:
    args["port"] = 3306
else:
    args["port"] = int(port)

args["user"] = os.environ.get("RS_USERNAME")
if args["user"] is None:
    print("RS_USERNAME must be set")
    os.Exit(2)

password = os.environ.get("RS_PASSWORD")
if password is not None:
    args["passwd"] = password

args["db"] = os.environ.get("RS_DATABASE")
if args["db"] is None:
    print("RS_DATABASE must be set")
    os.Exit(3)

connection = MySQLdb.connect(**args)


def check_people(expected_results):
    with connection.cursor() as cursor:
        query = "SELECT * FROM people ORDER BY id"
        cursor.execute(query)
        for result in expected_results:
            if cursor.fetchone() != result:
                return False
        if cursor.fetchone() is not None:
            return False
        return True


def eventually_consistent_people(expected_results, seconds):
    start = timeit.default_timer()
    while not check_people(expected_results):
        now = timeit.default_timer()
        if now - start > seconds:
            return False
        time.sleep(0.1)
    return True


with connection.cursor() as cursor:
    query = "CREATE TABLE people (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)"
    cursor.execute(query)
connection.commit()


with connection.cursor() as cursor:
    query = "INSERT INTO people VALUES (DEFAULT, %s)"
    cursor.execute(query, ("ReadySet User 1",))
    cursor.execute(query, ("ReadySet User 2",))
    cursor.execute(query, ("ReadySet User 3",))
connection.commit()


success = eventually_consistent_people(
    [(1, "ReadySet User 1"), (2, "ReadySet User 2"), (3, "ReadySet User 3")], 5
)
assert success


with connection.cursor() as cursor:
    query = "UPDATE people SET name = %s WHERE id = %s"
    cursor.execute(query, ("NotReadySet User", 2))
connection.commit()


success = eventually_consistent_people(
    [(1, "ReadySet User 1"), (2, "NotReadySet User"), (3, "ReadySet User 3")], 5
)
assert success


with connection.cursor() as cursor:
    query = "DELETE FROM people WHERE id = %s"
    cursor.execute(query, (2,))
connection.commit()


success = eventually_consistent_people(
    [(1, "ReadySet User 1"), (3, "ReadySet User 3")], 5
)
assert success


with connection.cursor() as cursor:
    query = "DROP TABLE people"
    cursor.execute(query)
connection.commit()
