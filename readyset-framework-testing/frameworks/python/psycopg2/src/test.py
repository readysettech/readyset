import os
import urllib.parse

import psycopg2

args = {}

args['host'] = os.environ.get('RS_HOST')
if(args['host'] is None):
	print('RS_HOST must be set')
	os.Exit(1)

port = os.environ.get('RS_PORT')
if(port is None):
	args['port'] = 5432
else:
	args['port'] = int(port)

args['user'] = os.environ.get('RS_USERNAME')
if(args['user'] is None):
	print('RS_USERNAME must be set')
	os.Exit(2)

password = os.environ.get('RS_PASSWORD')
if(password is not None):
	args['password'] = password

args['dbname'] = os.environ.get('RS_DATABASE')
if(args['dbname'] is None):
	print('RS_DATABASE must be set')
	os.Exit(3)

connection = psycopg2.connect(**args)
connection.set_session(autocommit=True)

with connection.cursor() as cursor:
	query = 'CREATE TABLE people (id SERIAL PRIMARY KEY, name TEXT NOT NULL)'
	cursor.execute(query)

with connection.cursor() as cursor:
	query = 'INSERT INTO people (name) VALUES (%s)'
	cursor.execute(query, ('ReadySet User 1',))
	cursor.execute(query, ('ReadySet User 2',))
	cursor.execute(query, ('ReadySet User 3',))

with connection.cursor() as cursor:
	query = 'SELECT * FROM people ORDER BY id ASC'
	cursor.execute(query)
	assert(cursor.fetchone() == (1, 'ReadySet User 1'))
	assert(cursor.fetchone() == (2, 'ReadySet User 2'))
	assert(cursor.fetchone() == (3, 'ReadySet User 3'))
	assert(cursor.fetchone() is None)

with connection.cursor() as cursor:
	query = 'UPDATE people SET name = %s WHERE id = %s'
	cursor.execute(query, ('NotReadySet User', 2))

with connection.cursor() as cursor:
	query = 'SELECT * FROM people ORDER BY id ASC'
	cursor.execute(query)
	assert(cursor.fetchone() == (1, 'ReadySet User 1'))
	assert(cursor.fetchone() == (2, 'NotReadySet User'))
	assert(cursor.fetchone() == (3, 'ReadySet User 3'))
	assert(cursor.fetchone() is None)

with connection.cursor() as cursor:
	query = 'DELETE FROM people WHERE id = %s'
	cursor.execute(query, (2,))

with connection.cursor() as cursor:
	query = 'SELECT * FROM people'
	cursor.execute(query)
	assert(cursor.fetchone() == (1, 'ReadySet User 1'))
	assert(cursor.fetchone() == (3, 'ReadySet User 3'))
	assert(cursor.fetchone() is None)

with connection.cursor() as cursor:
	query = 'DROP TABLE people'
	cursor.execute(query)
