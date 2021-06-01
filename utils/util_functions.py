import requests
from bs4 import BeautifulSoup


def request_url(url):
	try:
		with requests.Session() as session:
			response = session.get(url)
			return BeautifulSoup(response.content, 'html.parser')
	except Exception as e:
		print(e, "request_url")
		return None
