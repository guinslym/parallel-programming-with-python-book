#coding: utf-8

import sys, logging, re, threading, queue, requests, concurrent.futures

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


html_link_regex = \
        re.compile('<a\s(?:.*?\s)*?href=[\'"](.*?)[\'"].*?>')

urls = queue.Queue()
urls.put('http://www.google.com')
urls.put('http://br.bing.com/')
urls.put('https://duckduckgo.com/')
urls.put('https://github.com/')
urls.put('http://br.search.yahoo.com/')
result_dict = {}


def group_urls_task(urls):
    try:
        url = urls.get(True, 0.05)
        result_dict[url] = None
        logger.info("[%s] putting url [%s] in dictionary..." % (threading.current_thread().name, url))
    except queue.Empty:
        logging.error('Nothing to be done, queue is empty')


def crawl_task(url):
    links = []
    try:
        request_data = requests.get(url)
        logger.info("[%s] crawling url [%s] ..." % (
        threading.current_thread().name, url))
        links = html_link_regex.findall(request_data.text)
    except:
        logger.error(sys.exc_info()[0])
        raise
    finally:
        return (url, links)

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as group_link_threads:
    for i in range(urls.qsize()):
        group_link_threads.submit(group_urls_task, urls)

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as crawler_link_threads:
    future_tasks = {crawler_link_threads.submit(crawl_task, url): url for url in result_dict.keys()}
    for future in concurrent.futures.as_completed(future_tasks):
        result_dict[future.result()[0]] = future.result()[1]

for url, links in result_dict.items():
	logger.info("[%s] with links : [%s..." % (url, links[0]))
