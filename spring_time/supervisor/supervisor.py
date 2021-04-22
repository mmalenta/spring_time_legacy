import csv
import logging
import pika

import multiprocessing as mp

from astropy.time import Time
from json import loads
from socket import gethostname
from typing import Dict

from meertrig.voevent import VOEvent
from meertrig.config_helpers import get_config

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

    _voevent_defaults: Dict
      Default parameter values for the trigger. Contains information
      that should not really change between the triggers, such as
      the author contact details and some basic information about
      the backend.

  """

  def __init__(self, configuration: Dict):

    logger.debug("Starting the supervisor...")
    
    self._hostname = gethostname()
    self._voevent_defaults = get_config(configuration["voe_defaults"])
    self._voevent = VOEvent(host=configuration["voe_host"],
                            port=configuration["voe_port"])

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
    

    params = {
      'utc': Time.now().iso,
      'title': 'Detection of test event',
      'short_name': 'Test event',
      'beam_semi_major': 64.0 / 60.0,
      'beam_semi_minor': 28.0 / 60.0,
      'beam_rotation_angle': 0.0,
      'tsamp': 0.367,
      'cfreq': 1284.0,
      'bandwidth': 856.0,
      'nchan': 4096,
      'beam': 123,
      'dm': cand_data["dm"],
      'dm_err': 0.25,
      'width': 0.300,
      'snr': cand_data["snr"],
      'flux': 10,
      'ra': 20.4,
      'dec': 45.0,
      'gl': 10,
      'gb': 20,
      'name': "Source",
      'importance': 0.6,
      'internal': 1,
      'open_alert': 0,
      'test': 1,
      'product_id': "array_1",
    }

    params.update(self._voevent_defaults)

    event = self._voevent.generate_event(params, True)
    self._voevent.send_event(event)