#!/usr/bin/env python
import pika

class Credentials:
    def __init__(self, username, password, server, port, vhost):
        self._username = username
        self._password = password
        self._server = server
        self._port = port
        self._vhost = vhost

    @property
    def connection(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self._server,
                port=self._port,
                virtual_host=self._vhost,
                credentials=pika.PlainCredentials(self._username, self._password),
                socket_timeout=10)
        )
        return connection

class Consumer(Credentials):

    def __init__(self, username, password, server, port, vhost):
        super().__init__(username, password, server, port, vhost)
        self.channel = self.connection.channel()

    def on_message(self, channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def consume(self, queue):
        self.channel.basic_consume(queue, self.on_message)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        self.channel.close()

class Producer(Credentials):
    def __init__(self, username, password, server, port, vhost):
        super().__init__(username, password, server, port, vhost)
        self.channel = self.connection.channel()

    def producer(self, queue, durable, body):
        self.channel.queue_declare(queue=queue, durable=durable)
        self.channel.basic_publish(exchange='', routing_key=queue, body=body)
        self.connection.close()