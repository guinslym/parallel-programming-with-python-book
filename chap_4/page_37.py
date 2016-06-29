#coding: utf-8

import sys, logging, re, threading, queue, requests

"""
The concurrent.futures module provides a high-level interface for
asynchronously executing callables.
"""
import concurrent.futures
"""
In this scheme, we will use a very interesting Python resource,
ThreadPoolExecutor, which is featured in the concurrent.futures module.

In the previous example page_33.py, we had to create and initialize
more than one thread manually. In larger programs, it is very difficult to manage this
kind of situation. In such case, there are mechanisms that allow a thread pool. A thread
pool is nothing but a structure that keeps several threads, which are previously created,


In such case, there are mechanisms that allow a thread pool. A thread
pool is nothing but a structure that keeps several threads, which are previously created,
to be used in a certain process. It aims to reuse threads, thus avoiding unnecessary
creation of threads—which is costly.
"""

#logging configuration
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

#logging enable and ready to write
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

#Get all the links in the webpage
html_link_regex = \
        re.compile('<a\s(?:.*?\s)*?href=[\'"](.*?)[\'"].*?>')

"""
Following the sequence, we have populated a synchronized queue so that it simulates
certain input data. Then, we will declare a dictionary instance, which we will call
result_dict. In this, we will correlate the URLs and their respective links as a list
structure.
"""
urls = queue.Queue()
urls.put('http://www.google.com')
urls.put('http://br.bing.com/')
urls.put('https://duckduckgo.com/')
urls.put('https://github.com/')
urls.put('http://br.search.yahoo.com/')
result_dict = {}


def group_urls_task(urls):
"""
In the following piece of code, a function called group_urls_task is defined to
extract URLs from the synchronized queue to populate result_dict. We can see
that the URLs are keys of .

Another detail that we can observe is that the get()function was used with
two arguments. The first argument is True to block the access to a synchronized
queue. The second argument is a timeout of 0.05 to avoid this waiting
getting too long in case of nonexistence of elements in the synchronized queue.
In some cases, you do not want to spend too much time
blocked in waiting for elements. The code is as follows:
"""
    try:
        #The first argument is True to block the access to a synchronized queue.
        #The second argument is a timeout of 0.05 to avoid this waiting
        #getting too long
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
