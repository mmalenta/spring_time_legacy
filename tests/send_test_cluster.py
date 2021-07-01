import pika

from json import dumps
from socket import gethostname
from time import time

from astropy.time import Time
from numpy.random import ranf

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

cluster_size = 32
# Below parameters will be the same for all the members of the cluster
# Time for the "cluster centre"
main_time = time() + ranf() * 15 - 10
# DM for the "cluster centre"
main_dm = 123.45 + ranf() * 2.0 - 1.0

for cand in range(cluster_size):




  message = {
    "dm": main_dm + ranf() * 5.0 - 2.5,
    "mjd": Time(main_time + ranf() * 0.05, format="unix").mjd,
    "snr": 20.0 + ranf() * 5.0,
    "beam_abs": 25,
    "beam_type": "C",
    "ra": 2,
    "dec": 4,
    "time_sent": time(),
    "hostname": gethostname()
  }
  
  channel.basic_publish(exchange="post_processing", routing_key="clustering", body=dumps(message))
