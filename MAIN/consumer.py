import pika
import time
import socket

server = "localhost"
exchange_name = "dns_exchange"
request_queue = "dns_request_queue"
request_key = "dns_request"


# dns lookup
def dns_lookup(hostname):
    return socket.gethostbyname(hostname)


# callback function on receiving request messages, reply to the reply_to header
def on_message(channel, method, properties, body):
    print "received %s" % body
    ip_address = dns_lookup(body)

    print "ip address %s" % ip_address

    try:
        reply_prop = pika.BasicProperties(content_type="text/plain", delivery_mode=1)
        channel.basic_publish(exchange=exchange_name, routing_key=properties.reply_to, properties=reply_prop,
                              body="Reply to %s is %s" % (body, ip_address))
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except:
        channel.basic_nack(delivery_tag=method.delivery_tag)


def main():
    while True:
        try:
            # connect
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=server))
            channel = connection.channel()

            # declare exchange and queue, bind them and consume messages
            channel.exchange_declare(exchange=exchange_name, exchange_type="direct", auto_delete=True)
            channel.queue_declare(queue=request_queue, exclusive=True, auto_delete=True)
            channel.queue_bind(exchange=exchange_name, queue=request_queue, routing_key=request_key)
            channel.basic_consume(consumer_callback=on_message, queue=request_queue, no_ack=False)
            channel.start_consuming()
        except Exception, e:
            # reconnect on exception
            print "Exception handled, reconnecting...\nDetail:\n%s" % e
            try:
                connection.close()
            except:
                pass
            time.sleep(5)

if __name__ == '__main__':
    main()
