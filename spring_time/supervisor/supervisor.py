import csv
import logging
import pika

import multiprocessing as mp

from json import loads
from socket import gethostname
from typing import Dict

logger = logging.getLogger(__name__)

class Supervisor:

  """
  Main class for receiving and processing incoming information
  from the workers.

  Arguments:

    configuration: Dict
      Configuration passed to the supervisor.

  Attributes:

    _hostname: str
      Hostname for the machine running the supervisor.
      This can be different to the actual hostname
      that this code is running on if the supervisor is run inside a
      Docker container with --hostname option

  """

  def __init__(self, configuration: Dict):

    logger.debug("Starting the supervisor...")
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

    clustering_process = mp.Process(target=self.clustering_listen, args=(broker_configuration,))
    clustering_process.start()
    clustering_process.join()

  def clustering_listen(self, configuration: Dict) -> None:

    """
    Creates all the required connections and starts to listen to
    incoming messages for clustering.

    Arguments:

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

    channel.queue_declare("clustering", durable=False)
    channel.queue_bind("clustering", "post_processing")
    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(queue="clustering", auto_ack=False, on_message_callback=self.cluster)
    logger.info("Hello")
    logger.debug("Waiting for messages...")
    channel.start_consuming()


  def cluster(self, ch, method, properties, body):

    """
    Receives the incoming candidate information
    and runs some processing on that data

    This is a callback for the basic_consume pika functions.

    Arguments:

      All the arguments are the required arguments for a pika
      callback function.

    """

    cand_data = loads(body.decode("utf-8"))
    logger.info("Received candidate information %s", cand_data)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    self.trigger(cand_data, True)

  def trigger(self, cand_data: Dict, dummy: bool = False) -> None:

    """
    Sends a trigger to save the transient buffer data block with the
    candidate

    This method is called for every candidate that makes it
    through the various post-processing stages

    Arguments:

      dummy: bool, default False
        If set to 'true', sends a dummy trigger that does not
        save the transient buffer data. Setting it to 'false' (or just
        using the default value) will send a proper trigger.

      cand_data: Dict
        Candidate data required for triggering. Relevant information
        only is send with the proper trigger and all the imformation
        is send with the dummy trigger.

    Returns:

      None

    """

    if dummy:

      with open("clustering.out", "a") as cf:
        writer = csv.writer(cf, delimiter=" ")
        writer.writerow([cand_data["mjd"],
                          cand_data["dm"],
                          cand_data["snr"],
                          cand_data["beam_abs"],
                          cand_data["beam_type"],
                          cand_data["ra"],
                          cand_data["dec"],
                          cand_data["time_sent"]])
      