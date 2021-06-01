import sqlite3
from sqlite3 import Error

database = "../spider.db"


def create_connection():
	conn = None
	try:
		conn = sqlite3.connect(database)
	except Error as e:
		print(e)
	return conn


def create_views():
	conn = create_connection()
	view_1 = """SELECT DISTINCT UserName, PostCount FROM User ORDER BY PostCount DESC LIMIT 10;"""
	try:
		with conn:
			c = conn.cursor()
			c.execute(view_1)
			result = [username for username in c.fetchall()]

	except Error as e:
		print(e)

	for x, y in result:
		print(f'{y}\t\t{x}')


if __name__ == '__main__':
	create_views()