import pika
import thread
import time
import sys

server = "localhost"
exchange_name = "dns_exchange"
reply_queue = "dns_reply_queue"
request_key = "dns_request"
reply_key = "dns_reply"


# Parse and validate args
def parse_valid_args(args):
    if len(args) != 2:
        print "Usage: python producer.py domain.name "
        sys.exit(1)

    return args[1]


# callback function on receiving reply messages
def on_message(channel, method, properties, body):
    print body
    # close connection once receives the reply
    channel.stop_consuming()
    connection.close()


# listen for reply messages
def listen():
    channel.queue_declare(queue=reply_queue, exclusive=True, auto_delete=True)
    channel.queue_bind(exchange=exchange_name, queue=reply_queue, routing_key=reply_key)
    channel.basic_consume(consumer_callback=on_message, queue=reply_queue, no_ack=True)
    channel.start_consuming()


try:
    # validate args
    message = parse_valid_args(sys.argv)

    # connect
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=server))
    channel = connection.channel()

    thread.start_new_thread(listen, ())
    time.sleep(1)  # give time for it to start consuming

    print "sending message %s" % message
    # send request message
    properties = pika.spec.BasicProperties(content_type="text/plain", delivery_mode=1, reply_to=reply_key)
    channel.basic_publish(exchange=exchange_name, routing_key=request_key, body=message, properties=properties)

    # block until receives reply message
    while connection.is_open:
        pass
except Exception, e:
    print e
