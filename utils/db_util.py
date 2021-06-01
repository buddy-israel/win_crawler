import sqlite3
from sqlite3 import Error

database = "spider.db"


def create_connection():
	conn = None
	try:
		conn = sqlite3.connect(database)
	except Error as e:
		print(e)
	return conn


def create_trigger():
	conn = create_connection()
	with conn:
		trigger_1 = """
		CREATE TRIGGER IF NOT EXISTS update_user_table AFTER INSERT ON Posts
		BEGIN
			INSERT INTO User(UserName) 
			VALUES (NEW.PostAuthor);
			UPDATE User
			SET PostCount = (
			SELECT COUNT(*) 
			FROM Posts 
			WHERE UserName = PostAuthor)
		;END"""

		trigger_2 = """
		CREATE TRIGGER update_user_table_2 AFTER INSERT ON Comments
      	BEGIN
      		UPDATE User
      		SET CommentCount = (
      		SELECT COUNT(*)
      		FROM Comments
      		WHERE UserName = CommentAuthor)
      	;END"""

	try:
		c = conn.cursor()
		c.execute(trigger_1)
		c.execute(trigger_2)
		conn.commit()
	except Exception as e:
		print(e)


def create_tables():
	conn = create_connection()
	with conn:
		task_1 = """
		CREATE TABLE IF NOT EXISTS Posts (
			PostID VARCHAR PRIMARY KEY UNIQUE,
			PostAuthor VARCHAR,
			PostTitle VARCHAR,
			PostTimeStamp TEXT,
			PostCommentCount INT
			);"""

		task_2 = """
		CREATE TABLE IF NOT EXISTS User (
			UserName VARCHAR PRIMARY KEY UNIQUE,
			PostCount INT
			);"""

		task_3 = """
		CREATE TABLE IF NOT EXISTS Comments (
			CommentID VARCHAR PRIMARY KEY UNIQUE,
			ParentPostID VARCHAR,
			CommentAuthor VARCHAR,
			CommentBody VARCHAR,
			CommentTimeStamp TEXT
			);"""

		try:
			_extracted_from_create_tables_11(conn, task_1, task_2, task_3)
		except Exception as e:
			print(e)


def _extracted_from_create_tables_11(conn, task_1, task_2, task_3):
	c = conn.cursor()
	c.execute(task_1)
	c.execute(task_2)
	c.execute(task_3)
	conn.commit()
	create_trigger()


def update_post_table(posts):
	conn = create_connection()
	for post in posts:
		with conn:
			c = conn.cursor()
			c.execute("""
				INSERT OR IGNORE INTO
					Posts 
				VALUES (
					:PostID,
					:PostAuthor,
					:PostTitle,
					:PostTimeStamp,
					:PostCommentCount
					) """, {
				'PostID'          : post.PostID,
				'PostAuthor'      : post.PostAuthor,
				'PostTitle'       : post.PostTitle,
				'PostTimeStamp'   : post.PostTimeStamp,
				'PostCommentCount': post.PostCommentCount
			})
			conn.commit()


def update_comment_table(comments):
	conn = create_connection()
	for comment in comments:
		with conn:
			c = conn.cursor()
			c.execute("""
				INSERT OR IGNORE INTO
					Comments 
				VALUES (
					:CommentID,
					:ParentPostID,
					:CommentAuthor,
					:CommentBody,
					:CommentTimeStamp
					) """, {
				'CommentID'       : comment.CommentID,
				'ParentPostID'    : comment.ParentPostID,
				'CommentAuthor'   : comment.CommentAuthor,
				'CommentBody'     : comment.CommentBody,
				'CommentTimeStamp': comment.CommentTimeStamp
			})
			conn.commit()


def get_unique_usernames():
	conn = create_connection()
	task = """SELECT DISTINCT UserName FROM User"""
	try:
		with conn:
			c = conn.cursor()
			c.execute(task)
			return [username[0] for username in c.fetchall()]

	except Error as e:
		print(e)


def count_comments():
	conn = create_connection()
	task = """SELECT CommentCount FROM User"""
	try:
		with conn:
			c = conn.cursor()
			c.execute(task)
			return [int(c[0]) for c in c.fetchall()]

	except Error as e:
		print(e)
