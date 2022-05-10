import csv
import heapq
import logging
import numpy as np
import pika
import queue
import requests as req

from json import dumps
from numpy import abs
from scipy.optimize import newton
from scipy.special import erf
from socket import gethostname
from time import gmtime, strftime, time
from typing import Dict

from meertrig.voevent import VOEvent
from meertrig.config_helpers import get_config

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

    _sigma_limit: float
      SNR limit for the clustering DM offset.

    _alpha: float
      Constant coefficient that depends on _sigma limit. We do not (want to)
      calculate it every time we need it - use this precomputed value instead.

    _cluster_margin_s: float
      Time marging in seconds in front of the currently clustered oldest
      candidate to consider for clustering.

    _cluster_margin_mjd: float
      Same time marging as above, but converted to MJD. Candidate times are
      in MJD, so it helps us to avoid unnecessary conversions.

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
      
    IMPORTANT CLUSTERING VARIABLES:

    _dm_thresh: float, default 0.05
      Fractional DM tolerance used for matching

    _time_thresh: float [s], default 0.030 (30ms)
      Time tolerance used for matching

  """

  def __init__(self, configuration: Dict, cand_queue):

    logger.debug("Starting the clusterer...")

    self._candidate_queue = cand_queue
    self._hostname = gethostname()
    self._voevent_defaults = get_config(configuration["voe_defaults"])
    self._voevent = VOEvent(host=configuration["voe_host"],
                            port=configuration["voe_port"])

    self._buffer_wait_limit = 50
    self._cluster_wait_limit = 120

    self._cluster_candidates = []
    self._buffer_candidates = []

    self._dm_thresh = 0.05 
    self._time_thresh = 30e-03

    self._buffer_keys = None

    self._sigma_limit = 7.0
    self._alpha = np.sqrt(np.pi) / 2.0 / self._sigma_limit
    self._cluster_margin_s = 1.5
    self._cluster_margin_mjd = self._cluster_margin_s / 86400.0

  def _delta_dm(self, candidate: Dict) -> float:

    """
    
    Calculate the DM offset for a candidate SNR to reach self._sigma_limit.

    Calculates the approximate value for the DM offset based on the equations
    12 and 13 from "Searches for fast radio transients" by J.M Cordes and
    M. A. McLaughlin. Equation 12 is rearranged to derive equation
    zeta = erf(zeta) * beta (beta is used to simply keep all the constants
    together) for the point where the candidate SNR reaches the predefined
    self._sigma_limit. Newton-Raphson method is then used to obtain zeta
    when zeta - erf(zeta) * beta = 0 (we can use the fact that erf'(x) is a
    nice function).
    
    """

    beta = self._alpha * candidate["snr"]
    gamma = (6.91e-03 * candidate["bw_mhz"] 
            / candidate["width"] 
            / (candidate["cfreq_mhz"] / 1000.0)**3)

    f = lambda x, : x - erf(x) * beta
    fp = lambda x: 1 - 2.0 / np.sqrt(np.pi) * beta * np.e ** (-1.0 * x**2)

    # NOTE: This will be approaching zero from the side of the
    # candidate DM.
    # TODO: Need to check whether SNR at the candidate DM is below our
    # SNR limit. If it isn't we have to move the DM to a higher one as
    # we would end up with the final delta DM of 0.
    zeta = newton(f, gamma * candidate["dm"], fprime=fp)

    return zeta / gamma

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

        logger.info("Received candidate")
        logger.info(candidate)

        current_time = time()
        cand_time = Time(candidate["mjd"], format="mjd").unix

        # NOTE: This heavily relies on the ordering of the dictionary
        # not changing. Use Python 3.6+ ONLY
        # TODO: We will be adding extra keys
        if (self._buffer_keys == None):
          self._buffer_keys = tuple(candidate.keys())

        diff_time = current_time - cand_time
        logger.debug(diff_time)

        if (diff_time > self._cluster_wait_limit):

          """ 

          TODO: Decide what to do here exactly. This should be a rare
          occurance with candidate being close to 2 mimutes behind, so
          we should be able just to save them. HOWEVER that happens
          mostly when we have a lot of RFI, so it would be
          counterproductive to save a lot of RFI (ML might be enough
          to stop it though).

          """

          # This should not happen under ususal circumstances, so we
          # log is as a warning
          logger.warning("Time difference of %.2f!"
                          " Will not participate in clustering!", diff_time)
        
          try:
            channel.basic_publish(exchange="post_processing",
                                  routing_key="archiving_" + candidate["hostname"],
                                  body=dumps(candidate))
          except:
            logger.error("Resetting the lost RabbitMQ connection")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            channel = connection.channel()
            channel.basic_publish(exchange="post_processing",
                                  routing_key="archiving_" + candidate["hostname"],
                                  body=dumps(candidate))

        elif (diff_time > self._buffer_wait_limit):

          logger.warning("Time difference of %.2f!"
                          " Will not be saved by the TB!", diff_time)
          # There has to be a better way than constant data structure
          # swapping

          #candidate["delta_dm"] = self._delta_dm(candidate)

          heapq.heappush(self._cluster_candidates,
                          (cand_time, 
                          (candidate["mjd"],
                          candidate["dm"],
                          #candidate["delta_dm"],
                          candidate["snr"],
                          candidate["cand_hash"],
                          candidate["hostname"])))

        else:

          logger.debug("Time difference of %.2f."
                        " Will be considered for full TB clustering",
                        diff_time)

          #candidate["delta_dm"] = self._delta_dm(candidate)

          heapq.heappush(self._buffer_candidates, 
                          (cand_time,
                          tuple(candidate.values())))

          heapq.heappush(self._cluster_candidates, 
                          (cand_time, 
                          (candidate["mjd"],
                          candidate["dm"],
                          #candidate["delta_dm"],
                          candidate["snr"],
                          candidate["cand_hash"],
                          candidate["hostname"])))

      except queue.Empty:
        pass 

      if (len(self._buffer_candidates) > 0):

        current_time = time()
        oldest_buffer = self._buffer_candidates[0]
        oldest_buffer_time = oldest_buffer[0]
        oldest_buffer_dm = oldest_buffer[1][1]

        if ((current_time - oldest_buffer_time) >= self._buffer_wait_limit):
          logger.debug("Candidates ready for the Transient buffer clustering")

          # This will be a proper performance killer
          current_buffer = [candidate for candidate in self._buffer_candidates
                            if (abs(candidate[0] - oldest_buffer_time) <= self._time_thresh
                            and abs(candidate[1][1] - oldest_buffer_dm) <= self._dm_thresh * oldest_buffer_dm)]

          self._buffer_candidates = list(set(self._buffer_candidates)
                                          - set(current_buffer))

          logger.info("%d candidates in the TB cluster", len(current_buffer))

          logger.info("%d candidates left in the buffer queue",
                        len(self._buffer_candidates))

          # Get the highest SNR candidate within the cluster
          current_buffer = sorted(current_buffer, key = lambda cand: cand[1][2], 
                                  reverse=True)
          
          trigger_candidate = {x[0]: x[1] for x in zip(self._buffer_keys, current_buffer[0][1])}
          trigger_candidate["iso_t"] = Time(trigger_candidate["mjd"], format="mjd").iso
          self._trigger(trigger_candidate, True)

          oldest_buffer = []
          current_buffer = []

      if (len(self._cluster_candidates) > 0):

        current_time = time()
        oldest_cluster = self._cluster_candidates[0]
        oldest_cluster_time = oldest_cluster[0]
        oldest_cluster_dm = oldest_cluster[1][1]

        if ((current_time - oldest_cluster[0]) >= self._cluster_wait_limit):
          logger.debug("Candidates ready for the final clustering")

          """
            This clustering algorithm modifies the approach currently
            used for the MeerTRAB database by Fabian Jankowski
            The original code can be found under
            https://bitbucket.org/jankowsk/meertrapdb/src/master/
          """

          # This will be a proper performance killer
          current_cluster = [candidate for candidate in self._cluster_candidates
                            if (abs(candidate[0] - oldest_cluster_time) <= self._time_thresh
                            and abs(candidate[1][1] - oldest_cluster_dm) <= self._dm_thresh * oldest_cluster_dm)]

          self._cluster_candidates = list(set(self._cluster_candidates)
                                          - set(current_cluster))

          logger.info("%d candidates in the cluster", len(current_cluster))
          print(current_cluster)

          logger.info("%d candidates left in the clustering queue",
                        len(self._cluster_candidates))

          # Now sort the data by SNR and get the highest-SNR candidate
          # We send the highest-SNR candidate to the trigger
          # We send all the candidates to the archiving
          current_cluster = sorted(current_cluster, key = lambda cand: cand[1][2], 
                                  reverse=True)

          for cand in current_cluster:

            archive_dict = {
              "cand_hash": cand[1][3]
            }

            try:
              channel.basic_publish(exchange="post_processing",
                                    routing_key="archiving_" + cand[1][4],
                                    body=dumps(archive_dict))
            except:
              logger.error("Resetting the lost RabbitMQ connection")
              connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
              channel = connection.channel()
              channel.basic_publish(exchange="post_processing",
                                    routing_key="archiving_" + cand[1][4],
                                    body=dumps(archive_dict))

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
                          cand_data["iso_t"],
                          cand_data["dm"],
                          cand_data["snr"],
                          cand_data["beam_abs"],
                          cand_data["beam_type"],
                          cand_data["ra"],
                          cand_data["dec"],
                          cand_data["time_sent"]])

    else:    

      params = {
        'utc': cand_data["iso_t"],
        'title': 'Detection of test event',
        'short_name': 'Test event',
        'beam_semi_major': 64.0 / 60.0,
        'beam_semi_minor': 28.0 / 60.0,
        'beam_rotation_angle': 0.0,
        'tsamp': cand_data["tsamp_ms"],
        'cfreq': cand_data["cfreq_mhz"],
        'bandwidth': cand_data["bw_mhz"],
        'nchan': cand_data["nchan"],
        'beam': cand_data["beam_abs"],
        'dm': cand_data["dm"],
        'dm_err': 0.25,
        'width': cand_data["width"],
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

    self._send_trigger_notification(cand_data)

  def _send_trigger_notification(self, cand_data: Dict) -> None:

    """
    
    Sends a Slack message about the transient buffer trigger.

    Message contains some basic candidate information.

    Parameters:

      cand_data: Dict
        Candidate data required for triggering. Relevant information
        only is send with the proper trigger and all the imformation
        is send with the dummy trigger.

    Returns:

      None

    """

    message = {
      "pretext": f"* {strftime('%Y-%m-%d %H:%M:%S', gmtime())} NEW TB trigger* \n",
      "color": "#37961d",
      "text": (f"MJD: {cand_data['mjd']:.6f}\n"
               f"UTC: {cand_data['iso_t']}\n"
               f"DM: {cand_data['dm']:.2f} pc cm^-3\n"
               f"SNR: {cand_data['snr']:.2f}\n"
               f"Width: {cand_data['width']:.2f} ms\n"
               f"Beam: {cand_data['beam_abs']}\n"
               f"Beam type: {cand_data['beam_type']}\n"
               f"RA: {cand_data['ra']}\n"
               f"DEC: {cand_data['dec']}\n"
               f"Hostname: {cand_data['hostname']}"
      )
    }

    trigger_message = {
      "attachments": [message]
    }

    trigger_message_json = dumps(trigger_message)
    try:
      req.post("",
                data=trigger_message_json)
    except:
      logging.error("Could not send the Slack message! "
                    "Is the network connection down?")
    else:
      logging.debug("Slack message sent successfully")