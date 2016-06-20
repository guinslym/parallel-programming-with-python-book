#coding: utf-8
import os, random
from multiprocessing import Process, Pipe


def producer_task(conn):
    value = random.randint(1, 10)
    conn.send(value)
    print('Value [%d] sent by PID [%d]' % (value, os.getpid()))
    conn.close()

def consumer_task(conn):
    print('Value [%d] received by PID [%d]' % (conn.recv(), os.getpid()))

if __name__ == '__main__':
    producer_conn, consumer_conn = Pipe()
    consumer = Process(target=consumer_task, args=(consumer_conn,))
    producer = Process(target=producer_task, args=(producer_conn,))
    
    consumer.start()
    producer.start()
    
    consumer.join()
    producer.join()

