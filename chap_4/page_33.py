#coding: utf-8
import logging
"""
The threading module offers a friendly interface for the _thread module,
making its use more convenient compare to the _thread module.
"""
import threading


"""
The Queue module implements multi-producer, multi-consumer queues.
It is especially useful in threaded programming when information must
be exchanged safely between multiple threads. The Queue class in this
module implements all the required locking semantics. It depends on the
availability of thread support in Python; see the threading module.
"""
from queue import Queue

#logging configuration
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')

#logging enable and ready to write
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

fibo_dict = {}
shared_queue = Queue()
input_list = [3,10,5,7]

"""
In the following line of code, we will define an object from the threading module
called Condition. This object aims to synchronize the access to resources according
to a specific condition.
"""
queue_condition = threading.Condition()

def fibonacci_task(condition):
    """
    The next piece of code is a definition of the function to be
    executed by several threads. We will call it fibonacci_task.
    The fibonacci_task function receives the condition object as
    an argument that will control the fibonacci_task access to .
    """
    with condition:
        #The thread will wait until it gets notified that
        #shared_queue is free to process.
        while shared_queue.empty():
            logger.info("[%s] - waiting for the element in queue ... " % threading.current_thread().name)
            condition.wait()
        else:
            #Once we have the condition satisfied, the current thread
            #will obtain an element
            value = shared_queue.get()
            # calculates the Fibonacci series value and generates
            a,b = 0,1
            for item in range(value):
                a,b = b,a + b
                #an entry in the fibo_dict dictionary.
                fibo_dict[value] = a
        #aims to inform that a certain queued task has been extracted and excuted
        shared_queue.task_done()
        #logger.debug("[%s] fibonacci of key [%d] with result [%d]" % (threading.current_thread().name, value, fibo_dict[value]))
        logger.debug("[%s] - Result %s" % (threading.current_thread().name, fibo_dict))

def queue_task(condition):
    """
    The second function that we defined is the queue_task function
    that will be executed by the thread responsible for populating
    shared_queue with elements to be processed. We can notice the
    acquisition of received as an argument
    """
    logging.debug('Starting queue_task...')
    with condition:
        for item in input_list:
            shared_queue.put(item)
            logging.debug("Notifying fibonacci_task threads that the queue is ready to consume..")
            """
            After it inserts all the elements into shared_queue,
            the function notifies the threads responsible for
            calculating the Fibonacci series that the queue is
            ready to be used. notifyAll()
            """
            condition.notifyAll()

"""
In the next piece of code, we created a set of four threads that will wait for the
preparing condition from shared_queue.
"""
#threads = [threading.Thread(daemon=True, target=fibonacci_task,args=(queue_condition,)) for i in range(4)]
threads = []
for i in range(4):
    threads.append(
        threading.Thread(
                daemon=True,
                target=fibonacci_task,
                args=(queue_condition,)
        )
    )

"""
Then, we started the execution of the threads created to
calculate the Fibonacci serie by using the following code:

???why is it a list comprehension??? what is the output of this list
"""
#p = [thread.start() for thread in threads]
#print(p) => [None, None, None, None]
for thread in threads:
    thread.start()

"""
In the next step, we created a thread that will populate
shared_queue and start its execution.
"""
prod = threading.Thread(
            name='queue_task_thread',
            daemon=True,
            target=queue_task,
            args=(queue_condition,)
            )
prod.start()

"""
And finally, we called the join() method to all the threads that
calculate the Fibonacci series. The aim of this call is to make
the main thread wait for the execution of the Fibonacci series
from these threads so that it will not end the main flux of the
program before the end of their process.
"""
#[thread.join() for thread in threads]
for thread in threads:
    thread.join()

"""
RESULT:

2016-06-29 17:41:06,241 - [Thread-1] - waiting for the element in queue ...
2016-06-29 17:41:06,242 - [Thread-2] - waiting for the element in queue ...
2016-06-29 17:41:06,242 - [Thread-3] - waiting for the element in queue ...
2016-06-29 17:41:06,243 - [Thread-4] - waiting for the element in queue ...
2016-06-29 17:41:06,243 - Starting queue_task...
2016-06-29 17:41:06,243 - Notifying fibonacci_task threads that the queue is ready to consume..
2016-06-29 17:41:06,244 - Notifying fibonacci_task threads that the queue is ready to consume..
2016-06-29 17:41:06,244 - Notifying fibonacci_task threads that the queue is ready to consume..
2016-06-29 17:41:06,244 - Notifying fibonacci_task threads that the queue is ready to consume..
2016-06-29 17:41:06,245 - [Thread-3] - Result {3: 2}
2016-06-29 17:41:06,245 - [Thread-1] - Result {10: 55, 3: 2}
2016-06-29 17:41:06,245 - [Thread-2] - Result {10: 55, 3: 2, 5: 5}
2016-06-29 17:41:06,246 - [Thread-4] - Result {10: 55, 3: 2, 5: 5, 7: 13}


Processus ...(reminder)

fibonacci_task() ->
    queue_task() ->
        created a set of four threads ->
        started the execution of the threads created
                    to calculate the Fibonacci ->
        created a thread that will populate shared_queue
                    and start its execution ->
        we called the join() method
"""
