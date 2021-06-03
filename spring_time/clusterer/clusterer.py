import heapq
import logging
import pika
import queue

from json import dumps
from numpy import logical_and
from socket import gethostname
from time import sleep, time
from typing import Dict

from meertrig.voevent import VOEvent
from meertrig.config_helpers import get_config

from meertrapdb.clustering import Clusterer as MeerCluster

from astropy.time import Time

logger = logging.getLogger(__name__)

class Clusterer:

  """

  Class responsible for data clustering and sending triggers

  Parameters:

    cand_queue: multiprocessing.Queue
      Queue for exchanging the candidates between the supervisor and
      the clusterer.

    configuration: Dict
      Configuration passed to the supervisor.

  Attributes:
    
    _candidate_queue: multiprocessing.Queue
      Queue for exchanging the candidates between the supervisor and
      the clusterer.

    _cluster_candidates: priority queue
      Main store for all the candidates currently participating in the
      clustering. Simple priority queue with the candidate Unix
      timestamp used as 'priority' (easier to do calculations this
      way rather than using the candidate MJD).

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

    _voevent: VOEvent
      Class for handling VOEvents. Responsible for sending the evnets
      to the broker.

    IMPORTANT TIME VARIABLES:

    _buffer_wait_limit: float [s]
      Number of seconds that the data will be kept the Transient Buffer
      save. This has to be close to the length of the Buffer,
      but ideally few seconds to allow for clustering and triggering.
      For the candidate to be properly saved by the Buffer the
      difference between its time of appearance (time of the actual
      event, not the time when it was picked up by our pipeline) and
      the current time HAS TO BE LESS than the size of the Buffer.

    _cluster_wait_limit: float [s]
      Number of seconds that the data will be kept in order to
      participate in the clustering. If data is present after that
      time limit it is simply archived on the nodes without any
      additional clustering. Ideally, this should not be used too often
      as this will be an indication of pipeline falling behind.
      This number does not have to be as scrict at the Transient Buffer
      time limit. It is recommended to be longer that the Transient
      Buffer time limit to allow for any extra candidates to arrive,
      but not too long as to keep too many candidates in memory.
      

  """

  def __init__(self, configuration: Dict, cand_queue):

    logger.debug("Starting the clusterer...")

    self._candidate_queue = cand_queue
    self._hostname = gethostname()
    self._voevent_defaults = get_config(configuration["voe_defaults"])
    self._voevent = VOEvent(host=configuration["voe_host"],
                            port=configuration["voe_port"])

    self._buffer_wait_limit = 60
    self._cluster_wait_limit = 120

    self._cluster_candidates = []
    self._buffer_candidates = []

  def cluster(self) -> None:

    """

    Receives the candidates from the supervisor and runs the clustering.

    Returns:

      None

    """

    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    while True:

      try:

        candidate = self._candidate_queue.get_nowait()

        logger.debug("Received candidate")
        logger.debug(candidate)

        current_time = time()
        cand_time = Time(candidate["mjd"], format="mjd").unix

        diff_time = current_time - cand_time
        logger.info(diff_time)

        if (diff_time > self._cluster_wait_limit):

          logger.warning("Time difference of %.2f!"
                          " Will not participate in clustering!", diff_time)

          """ 

          TODO: Decide what to do here exactly. This should be a rare
          occurance with candidate being close to 2 mimutes behind, so
          we should be able just to save them. HOWEVER that happens
          mostly when we have a lot of RFI, so it would be
          counterproductive to save a lot of RFI (ML might be enough
          to stop it though).

          """
        
          # As a test just send straight back for now
          channel.basic_publish(exchange="post_processing",
                                routing_key="archiving_" + candidate["hostname"],
                                body=dumps(candidate))

        elif (diff_time > self._buffer_wait_limit):

          logger.warning("Time difference of %.2f!"
                          " Will not be saved by the TB!", diff_time)
          heapq.heappush(self._cluster_candidates, 
                          (cand_time,
                          (candidate["mjd"],
                          candidate["dm"],
                          candidate["beam_abs"])))

        else:

          logger.debug("Time difference of %.2f."
                        " Will be considered for full TB clustering",
                        diff_time)
          heapq.heappush(self._buffer_candidates, 
                          (cand_time,
                          (candidate["mjd"],
                          candidate["dm"],
                          candidate["snr"],
                          candidate["beam_abs"])))

          heapq.heappush(self._cluster_candidates, 
                          (cand_time,
                          (candidate["mjd"],
                          candidate["dm"],
                          candidate["snr"],
                          candidate["beam_abs"])))

      except queue.Empty:
        logger.debug("No candidate yet...")

      """

      TODO: Do the work here. We need to check if the data can be
      clustered independently of receiving or not receiving a new
      candidate

      """

      if (len(self._buffer_candidates) > 0):

        current_time = time()
        oldest_buffer = self._buffer_candidates[0]
      
        if ((current_time - oldest_buffer[0]) >= self._buffer_wait_limit):
          logger.debug("Candidates ready for Transient Buffer clustering")

          # This is a very simple implementation of grid clustering
          # Fractional tolerance
          dm_thresh = 0.02
          # Tolerance in s (10ms)
          time_thresh = 10e-03

          tmp_candidates = sorted(self._buffer_candidates)

          cluster_mask = logical_and()


      if (len(self._cluster_candidates) > 0):

        current_time = time()
        oldest_cluster = self._cluster_candidates[0][0]
        if ((current_time - oldest_cluster) >= self._cluster_wait_limit):
          logger.debug("Candidates ready for the final clustering")

      logger.debug("Current buffer candidates %s", self._buffer_candidates)
      sleep(2)

  def _trigger(self, cand_data: Dict, dummy: bool = False) -> None:

    """
    Sends a trigger to save the transient buffer data block with the
    candidate

    This method is called for every candidate that makes it
    through the various post-processing stages

    Parameters:

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