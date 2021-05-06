import pika

from json import dumps
from time import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
message = {
  "dm": 123.45,
  "mjd": 59090.123456,
  "snr": 20.0,
  "beam_abs": 25,
  "beam_type": "C",
  "ra": 2,
  "dec": 4,
  "time_sent": time()
}
channel.basic_publish(exchange="post_processing", routing_key="clustering", body=dumps(message))
