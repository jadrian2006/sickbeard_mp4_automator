#!/usr/bin/env python

import threading
import pika
import os
import sys
import json
import time
#from autoprocess import autoProcessTV, autoProcessMovie, autoProcessTVSR, sonarr, radarr
from readSettings import ReadSettings
from mkvtomp4 import MkvtoMp4
from deluge_client import DelugeRPCClient
import logging
from logging.config import fileConfig
import delugePostProcess as delugePost


###################################
RABBIT_SERVER = '172.16.100.11'
RABBIT_PORT = '5672'
RABBIT_QUEUE = 'task_queue'
RABBIT_USER = 'guest'
RABBIT_PASS = 'guest'
# REQUEUE COUNT number of requeues of failed conversions, 0 for unlimited.
REQUEUE_COUNT = 0

###################################

logpath = './logs/rmq'
if os.name == 'nt':
    logpath = os.path.dirname(sys.argv[0])
elif not os.path.isdir(logpath):
    try:
        os.mkdir(logpath)
    except:
        logpath = os.path.dirname(sys.argv[0])
configPath = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), 'logging.ini')).replace("\\", "\\\\")
logPath = os.path.abspath(os.path.join(logpath, 'index.log')).replace("\\", "\\\\")
fileConfig(configPath, defaults={'logfilename': logPath})
logger = logging.getLogger("rabbit_consumer")

logger.info("Rabbit Consumer Log Started.")



class rabbitConsumer(object):
    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'task_queue'
    DURABLE = 'True'
    ROUTING_KEY = 'task_queue'

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self.info = self.__class__.__name__
        self.logger = logging.LoggerAdapter(logger, self.info)

    def connect(self):
        logger.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),self.on_connection_open,stop_ioloop_on_close=False)

    def close_connection(self):
        log.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning('Connection closed, reopening in 5 seconds: (%s) %s', reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        logger.warning('Channel %i was closed: (%s) %s', channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        logger.info('Exchange declared')
        self.setup_queue(self.QUEUE, self.DURABLE)

    def setup_queue(self, queue_name, queue_durable):
        logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, queue_durable)

    def on_queue_declareok(self, method_frame):
        logger.info('Binding %s to %s with %s', self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE, self.EXCHANGE, self.ROUTING_KEY)

    def add_on_cancel_callback(self):
        logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        logger.info('Consumer was cancelled remotely, shutting down: %r',method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        logger.info('Acknowledging message %s', delivery_tag)
        self.retry = False
        self._channel.basic_ack(delivery_tag)

    def nack_message(self, delivery_tag):
        logger.info('Acknowledging message %s', delivery_tag)
        self.retry = True
        time.sleep(5)
        self._channel.basic_nack(delivery_tag, requeue = True)

    def requeue_message(self, body, delivery_tag):
        if self.priority == None:
            self.priority = 1
        else:
            self.priority += 1
        body = json.dumps(body)
        if REQUEUE_COUNT == 0:
            logger.info('Requeued failed request for retry #%s', self.priority)
            self._channel.basic_publish(exchange='', routing_key=self.ROUTING_KEY, body=body, properties=pika.BasicProperties(delivery_mode=2,priority=self.priority,))
            time.sleep(10)
            self.acknowledge_message(delivery_tag)
        elif self.priority <= REQUEUE_COUNT:
            logger.info('Requeued failed request for retry #%s', self.priority)
            self._channel.basic_publish(exchange='', routing_key=self.ROUTING_KEY, body=body, properties=pika.BasicProperties(delivery_mode=2,priority=self.priority,))
            time.sleep(10)
            self.acknowledge_message(delivery_tag)
        else:
            self.acknowledge_message(delivery_tag)

    def t_spawn_on_message(self, body, delivery_tag):
        logger.info('Worker thread started with %s', body)
        logger.info('Delivery Tag information %s', delivery_tag)
        body = json.loads(body)

        self.choose_source(body,delivery_tag)
        #self.acknowledge_message(delivery_tag)

    def choose_source(self, body, delivery_tag):
        if 'deluge' in body[1]:
            logger.info('We are going to process a deluge request')
            parms = ["",body[1]['deluge']['torrent_id'],body[1]['deluge']['torrent_name'],body[1]['deluge']['path']]
            logger.info('Our parameters are %s', parms)
            try:
                delugePost.main(parms)
                return self.acknowledge_message(delivery_tag)
            except:
                logger.error('Deluge client is not responding, requeing message and waiting 10 seconds')
                self.requeue_message(body, delivery_tag)
                return

        elif 'manual' in body[1]:
            return
        elif 'nzbget' in body[1]:
            return
        elif 'sabnzbd' in body[1]:
            return
        else:
            return


    def on_message(self, unused_channel, basic_deliver, properties, body):
        logger.info('Received message # %s from %s: %s', basic_deliver.delivery_tag, properties.app_id, body)
        self.priority = properties.priority
        t = threading.Thread(target=self.t_spawn_on_message, args=(body, basic_deliver.delivery_tag,))
        t.start()
        threads.append(t)

    def on_cancelok(self, unused_frame):
        logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._channel.basic_qos(prefetch_count=1)
        self._consumer_tag = self._channel.basic_consume(self.on_message, self.QUEUE)

    def on_bindok(self, unused_frame):
        logger.info('Queue bound')
        self.start_consuming()

    def close_channel(self):
        logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        logger.info('Stopped')

threads = []
def main():
    url = 'amqp://' + RABBIT_USER + ':' + RABBIT_PASS + '@' + RABBIT_SERVER + ':' + RABBIT_PORT + '/%2F'
    rmq = rabbitConsumer(url)
    try:
        rmq.run()

    except KeyboardInterrupt:
        rmq.stop()


if __name__ == '__main__':
    main()
