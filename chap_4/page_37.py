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
        #??? why  None...
        result_dict[url] = None
        logger.info("[%s] putting url [%s] in dictionary..." % (threading.current_thread().name, url))
    except queue.Empty:
        logging.error('Nothing to be done, queue is empty')


def crawl_task(url):
    """
    Now, we have the task that is responsible for accomplishing the crawling stage
    for each URL sent as an argument for the crawl_task function. Basically, the crawling
    stage is completed by obtaining all the links inside the page pointed by URL received.
    """
    links = []
    try:
        request_data = requests.get(url)
        logger.info("[%s] crawling url [%s] ..." % (threading.current_thread().name, url))
        links = html_link_regex.findall(request_data.text)
    except:
        logger.error(sys.exc_info()[0])
        raise
    finally:
        """
        A tuple returned by crawling contains the first element as a URL
        received by the crawl_task function.
        """
        return (url, links)

"""
class concurrent.futures.ThreadPoolExecutor(max_workers=None):
    An Executor subclass that uses a pool of at most max_workers threads
    to execute calls asynchronously.
"""
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as group_link_threads:
    """
    In the ThreadPoolExecutor object's scope, we iterate it in the synchronized
    queue and dispatch it to execute a reference for the queue containing URLs by means
    of the submit method. Summing up, the submit method schedules a callable for
    the execution and returns a Future object containing the scheduling created for the
    execution.

    """
    for i in range(urls.qsize()):
        """
        The submit method receives a callable and its arguments; in our case,
        the callable is the task group_urls_task and the argument is a reference to our
        synchronized queue. After these arguments are called, worker threads defined in the
        pool will execute the bookings in a parallel, asynchronous way.
        """
        group_link_threads.submit(group_urls_task, urls)

"""
After the previous code, we created another ThreadPoolExecutor; but this time, we
want to execute the crawling stage by using the keys generated by group_urls_task
in the previous stage.
"""
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as crawler_link_threads:
    """
    This time, there is a difference in the following line: (future_tasks =...)
    """
    future_tasks = {crawler_link_threads.submit(crawl_task, url): url for url in result_dict.keys()}
    """
    We have mapped a temporary dictionary called future_tasks. It will contain the
    bookings made by submit, passing by each URL featured in result_dict. That
    is, for each key, we create an entry in future_tasks. After mapping, we need to
    collect the results from the bookings as they are executed using a loop, which seeks
    completed entries in future_tasks using the concurrent.futures.as_completed
    (fs, timeout=None) method. This call returns an iterator for instances of the
    Future object. So, we can iterate in each result processed by the bookings that
    have been dispatched.
    """
    for future in concurrent.futures.as_completed(future_tasks):
        """
        At the end of ThreadPoolExecutor, for the crawling threads,
        we use the result() method from the Future object. In the case of the crawling
        stage, it returns the resulting tuple.
        """
        result_dict[future.result()[0]] = future.result()[1]

for url, links in result_dict.items():
	logger.info("[%s] with links : [%s..." % (url, links[0]))


"""
Result

-> % python page_37.py
2016-06-29 18:32:02,415 - [Thread-1] putting url [http://www.google.com] in dictionary...
2016-06-29 18:32:02,416 - [Thread-1] putting url [http://br.bing.com/] in dictionary...
2016-06-29 18:32:02,417 - [Thread-2] putting url [https://duckduckgo.com/] in dictionary...
2016-06-29 18:32:02,417 - [Thread-1] putting url [https://github.com/] in dictionary...
2016-06-29 18:32:02,417 - [Thread-3] putting url [http://br.search.yahoo.com/] in dictionary...
2016-06-29 18:32:02,445 - Starting new HTTP connection (1): www.google.com
2016-06-29 18:32:02,445 - Starting new HTTPS connection (1): github.com
2016-06-29 18:32:02,447 - Starting new HTTPS connection (1): duckduckgo.com
2016-06-29 18:32:02,547 - "GET / HTTP/1.1" 302 258
2016-06-29 18:32:02,549 - Starting new HTTP connection (1): www.google.ca
2016-06-29 18:32:02,704 - "GET /?gfe_rd=cr&ei=Ykx0V7_2JMSh8we4uqSoAg HTTP/1.1" 200 4733
2016-06-29 18:32:02,707 - [Thread-5] crawling url [http://www.google.com] ...
2016-06-29 18:32:02,709 - Starting new HTTP connection (1): br.search.yahoo.com
2016-06-29 18:32:02,715 - "GET / HTTP/1.1" 200 None
2016-06-29 18:32:02,719 - [Thread-6] crawling url [https://duckduckgo.com/] ...
2016-06-29 18:32:02,722 - Starting new HTTP connection (1): br.bing.com
2016-06-29 18:32:02,729 - "GET / HTTP/1.1" 200 None
2016-06-29 18:32:02,768 - [Thread-4] crawling url [https://github.com/] ...
2016-06-29 18:32:02,826 - "GET / HTTP/1.1" 302 0
2016-06-29 18:32:02,828 - Starting new HTTP connection (1): www.bing.com
2016-06-29 18:32:02,896 - "GET / HTTP/1.1" 200 5862
2016-06-29 18:32:02,901 - [Thread-5] crawling url [http://br.search.yahoo.com/] ...
2016-06-29 18:32:02,953 - "GET /?cc=br HTTP/1.1" 200 30346
2016-06-29 18:32:02,991 - [Thread-6] crawling url [http://br.bing.com/] ...
2016-06-29 18:32:02,992 - [https://github.com/] with links : [#start-of-content...
2016-06-29 18:32:02,992 - [http://www.google.com] with links : [http://www.google.ca/imghp?hl=en&tab=wi...
2016-06-29 18:32:02,992 - [https://duckduckgo.com/] with links : [/about...
2016-06-29 18:32:02,992 - [http://br.search.yahoo.com/] with links : [https://br.yahoo.com/...
2016-06-29 18:32:02,992 - [http://br.bing.com/] with links : [javascript:void(0)...
"""
