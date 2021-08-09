import csv
import logging
import pika

import multiprocessing as mp

from astropy.time import Time
from json import loads
from socket import gethostname
from typing import Dict

from clusterer.clusterer import Clusterer

logger = logging.getLogger(__name__)

class Supervisor:

  """

  Main class for receiving and processing incoming information
  from the workers.

  Parameters:

    configuration: Dict
      Configuration passed to the supervisor.

  Attributes:

    _candidate_queue: multiprocessing.Queue
      Queue for exchanging the candidates between the supervisor and
      the clusterer.

    _clusterer: Clusterer
      Class responsible for actual clustering and triggering. Picks up
      the data passed by the supervisor and works on it. Clustering runs
      in a separate process, independently of the supervisor listening
      to incoming candidates.

    _hostname: str
      Hostname for the machine running the supervisor.
      This can be different to the actual hostname
      that this code is running on if the supervisor is run inside a
      Docker container with --hostname option

    _voevent_defaults: Dict
      Default parameter values for the trigger. Contains information
      that should not really change between the triggers, such as
      the author contact details and some basic information about
      the backend.


  """

  def __init__(self, configuration: Dict):

    logger.debug("Starting the supervisor...")
    
    self._candidate_queue = mp.Queue()
    self._clusterer = Clusterer(configuration, self._candidate_queue)
    self._hostname = gethostname()

  def supervise(self) -> None:

    """
    Responsible for managing all the supervisor processes

    Starts the required supervisor processes
    and waits until they are done

    Returns: None

    """

    broker_configuration = {
      "hostname": self._hostname
    }

    listen_process = mp.Process(target=self.clustering_listen, args=(broker_configuration,))
    cluster_process = mp.Process(target=self._clusterer.cluster)
    listen_process.start()
    cluster_process.start()
    listen_process.join()
    cluster_process.join()


  def clustering_listen(self, configuration: Dict) -> None:

    """
    Creates all the required connections and starts to listen to
    incoming messages for clustering.

    Parameters:

      configuration: Dict
        Congurations parameters for message brokerage

    Returns: None

    """

    logger.debug("Seting up broker connections...")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange="post_processing",
                              exchange_type="direct",
                              durable=True)

    channel.queue_declare("clustering", durable=True)
    channel.queue_bind("clustering", "post_processing")
    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(queue="clustering", auto_ack=False, on_message_callback=self._send_cluster)
    logger.debug("Waiting for messages...")
    channel.start_consuming()


  def _send_cluster(self, ch, method, properties, body):

    """
    Receives the incoming candidate information and sends it to the
    clusteter.

    This is a callback for the basic_consume pika functions. Incoming
    candidate is passed to the candidate queue with the candidate
    later picked up by the clusterer.

    Arguments:

      All the arguments are the required arguments for a pika
      callback function.

    """

    cand_data = loads(body.decode("utf-8"))
    ch.basic_ack(delivery_tag=method.delivery_tag)

    self._candidate_queue.put_nowait(cand_data)