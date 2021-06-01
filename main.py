from utils import db_util as db, util_functions as util
import concurrent.futures


class GlobalCountClass:
	def __init__(self):
		self.Count = 0
		self.Total = 0

	def raise_count(self):
		self.Count += 1


class PostClass:

	def __init__(self, post):
		self.post = post

	@property
	def PostAuthor(self):
		try:
			return self.post.get('data-author')
		except:
			return None

	@property
	def PostUrl(self):
		try:
			return self.post.find("div", "top").a.get('href')
		except:
			return None

	@property
	def PostTimeStamp(self):
		try:
			return self.post.find("time", "timeago").get("datetime")
		except:
			return None

	@property
	def PostTitle(self):
		try:
			return self.post.find("div", "top").a.get_text().replace('\n', ' ').replace('\t', '').strip()
		except:
			return None

	@property
	def PostCommentCount(self):
		try:
			return self.post.find("a", class_="original comments").get_text().split(" ")[0]
		except:
			return None

	@property
	def PostID(self):
		if self.PostUrl is not None:
			return self.PostUrl.split('/')[2]


class FrontPageClass:

	def __init__(self, url):
		self.url = url
		self.base_url = url

	@staticmethod
	def find_next_url(soup):
		try:
			return soup.find("a", "next-page").get('href')
		except AttributeError:
			return None

	def crawl_front_page(self):

		print(f'Starting FrontPage-Crawler\n==========================')
		count = GlobalCountClass()

		while True:
			soup = util.request_url(self.url)
			next_url = self.find_next_url(soup)
			posts = [PostClass(post) for post in soup.find_all("div", "post")]
			count.raise_count()
			count.Total += len(posts)
			db.update_post_table(posts)
			print(f'parsed {count.Count} pages\nFound total of {count.Total} Posts\nNext Page is:\n{next_url}\n')

			if next_url is not None:
				self.url = f'{self.base_url}{next_url}'
				print(self.url)
			else:
				break


class UserCommentClass:

	def __init__(self, username, comment):
		self.comment = comment
		self.CommentAuthor = username

	@property
	def CommentID(self):
		try:
			return self.comment.find("div", "actions").find("a").get("href").split("/")[-1]
		except:
			return None

	@property
	def ParentPostID(self):
		try:
			return self.comment.find("div", "comment-parent").find(class_="title").find("a").get("href").split("/")[2]
		except:
			return None

	@property
	def CommentTimeStamp(self):
		try:
			return self.comment.find("div", "details").find("time", "timeago").get("datetime")
		except:
			return None

	@property
	def CommentBody(self):
		try:
			return self.comment.find("div", "content").p.get_text()
		except:
			return None


class UserClass:

	def __init__(self, username):
		self.username = username
		self.posts = []

	# self.comments = []

	def crawl_posts(self):
		count = GlobalCountClass()

		while True:
			count.raise_count()
			url = f'https://conspiracies.win/u/{self.username}/?type=post&sort=new&page={count.Count}'

			soup = util.request_url(url)
			if soup.find("div", "empty"):
				break

			posts = [PostClass(post) for post in soup.find_all("div", "post")]
			for p in posts:
				self.posts.append(p)

		print(f'Found: {count.Count} Post-Pages for User:{self.username}')
		db.update_post_table(self.posts)

	# def crawl_comments(self):
	# 	count = 0
	#
	# 	while True:
	# 		count += 1
	# 		url = f'https://conspiracies.win/u/{self.username}/?type=comment&sort=new&page={count}'
	#
	# 		soup = util.request_url(url)
	# 		if soup.find("div", "empty"):
	# 			break
	#
	# 		for _comment in soup.find_all("div", "comment"):
	# 			comment = UserCommentClass(self.username, _comment)
	# 			self.comments.append(comment)
	#
	# 	print(f'Found: {count} Comment-Pages for User:{self.username}')
	# 	db.update_comment_table(self.comments)

	def crawl_comments(self):
		count = 0

		while True:
			count += 1
			url = f'https://conspiracies.win/u/{self.username}/?type=comment&sort=new&page={count}'
			print(f'{self.username} : {count}')

			soup = util.request_url(url)
			if soup.find("div", "empty"):
				break

			comments = [UserCommentClass(self.username, _comment) for _comment in soup.find_all("div", "comment")]
			db.update_comment_table(comments)


def threaded_crawl():
	def crawl(user):
		user.crawl_comments()
		user.crawl_posts()

		GlobalCount.raise_count()
		print(f'{GlobalCount.Count}/{GlobalCount.Total}')

	unique_usernames = [UserClass(name) for name in db.get_unique_usernames()]
	GlobalCount.Total = len(unique_usernames)

	with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
		executor.map(crawl, unique_usernames)


GlobalCount = GlobalCountClass()

if __name__ == '__main__':
	db.create_tables()
	FrontPageCrawler = FrontPageClass('https://conspiracies.win/new')
	FrontPageCrawler.crawl_front_page()
	threaded_crawl()
	print(sum(db.count_comments()))
