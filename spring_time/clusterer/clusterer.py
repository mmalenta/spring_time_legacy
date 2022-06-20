import csv
from hashlib import new
import heapq
import logging
import numpy as np
import pika
import queue
import requests as req

from dotenv import dotenv_values
from json import dumps
from numpy import abs
from scipy.optimize import newton
from scipy.special import erf
from socket import gethostname
from time import gmtime, strftime, time
from typing import Dict

from meertrig.voevent import VOEvent
from meertrig.config_helpers import get_config

from astropy import units
from astropy.coordinates import SkyCoord
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

    self._buffer_wait_limit = 40
    self._cluster_wait_limit = 120

    self._cluster_candidates = []
    self._buffer_candidates = []

    self._dm_thresh = 0.05 
    self._time_thresh = 30e-03

    self._buffer_keys = None

    self._sigma_limit = 7.0
    self._alpha = np.sqrt(np.pi) / 2.0 / self._sigma_limit
    self._cluster_margin_s = 1
    self._cluster_margin_mjd = self._cluster_margin_s / 86400.0

    self._unix_time_index = -1
    self._mjd_index = -1
    self._dm_index = -1
    self._snr_index = -1
    self._width_index = -1
    self._delta_dm_index = -1

    self._slack_hook = dotenv_values(".env")["SLACK_HOOK"]

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

    # We need to make sure that the limiting DM for the NR method is below 
    # our sigma limit - the calculation will fail otherwise
    limit_dm = candidate["dm"]

    beta = self._alpha * candidate["snr"]
    gamma = (6.91e-03 * candidate["bw_mhz"] 
            / candidate["width"] 
            / (candidate["cfreq_mhz"] / 1000.0)**3)

    limit_snr = candidate["snr"] * np.sqrt(np.pi) / 2.0 / (gamma * limit_dm) * erf(gamma * limit_dm)

    while limit_snr >= self._sigma_limit:
      logger.debug("DM delta of %.3f results in SNR of %.2f. Doubling the delta limit", limit_dm, limit_snr)
      limit_dm = 2 * limit_dm
      limit_snr = candidate["snr"] * np.sqrt(np.pi) / 2.0 / (gamma * limit_dm) * erf(gamma * limit_dm)
    
    logger.debug("DM delta of %.3f results in SNR of %.2f", limit_dm, limit_snr)

    f = lambda x, : x - erf(x) * beta
    fp = lambda x: 1 - 2.0 / np.sqrt(np.pi) * beta * np.e ** (-1.0 * x**2)

    # NOTE: This will be approaching zero from the side of the
    # candidate DM.
    # TODO: Need to check whether SNR at the candidate DM is below our
    # SNR limit. If it isn't we have to move the DM to a higher one as
    # we would end up with the final delta DM of 0.
    zeta = newton(f, gamma * limit_dm, fprime=fp)

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

        cand_unix_time = Time(candidate["mjd"], format="mjd").unix

        # NOTE: This heavily relies on the ordering of the dictionary
        # not changing. Use Python 3.6+ ONLY
        # TODO: We will be adding extra keys
        if (self._buffer_keys == None):
          # NOTE: Any reason this is a tuple and not a list?
          #self._buffer_keys = tuple(candidate.keys())
          self._buffer_keys = ["unix_time"] + list(candidate.keys()) + ["delta_dm"]
          logger.info("Retrieved candidate dictionary keys: %r", self._buffer_keys)
          self._unix_time_index = self._buffer_keys.index("unix_time")
          self._mjd_index = self._buffer_keys.index("mjd")
          self._dm_index = self._buffer_keys.index("dm")
          self._snr_index = self._buffer_keys.index("snr")
          self._width_index = self._buffer_keys.index("width")
          self._delta_dm_index = self._buffer_keys.index("delta_dm")

        current_time = time()
        diff_time = current_time - cand_unix_time

        if (diff_time > self._cluster_wait_limit):

          """ 

          TODO: Decide what to do here exactly. This should be a rare
          occurance with candidate being close to 2 mimutes behind, so
          we should be able just to save them. HOWEVER that happens
          mostly when we have a lot of RFI, so it would be
          counterproductive to save a lot of RFI (ML might be enough
          to stop it though).

          """

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

        else:

          # This has to be done in any case
          candidate["delta_dm"] = self._delta_dm(candidate)
          heapq.heappush(self._cluster_candidates,
                        (cand_unix_time,) + tuple(candidate.values()))

          if (diff_time > self._buffer_wait_limit):
            logger.warning("Time difference of %.2f!"
                          " Will not be saved by the TB!", diff_time)
          else:
            heapq.heappush(self._buffer_candidates, 
                          (cand_unix_time,) + tuple(candidate.values()))
            logger.debug("Time difference of %.2f."
                        " Will be considered for full TB clustering",
                        diff_time)

      except queue.Empty:
        pass 

      if (len(self._buffer_candidates) > 0):

        current_time = time()
        oldest_buffer = self._buffer_candidates[0]
        oldest_buffer_unix_time = oldest_buffer[self._unix_time_index]

        if ((current_time - oldest_buffer_unix_time) >= self._buffer_wait_limit):
          logger.debug("Candidates ready for the Transient buffer clustering")

          # NOTE: Only get candidates within the clustering margin
          # This can (and will be) adjusted in the future
          current_buffer = [candidate for candidate in self._buffer_candidates
                            if (abs(candidate[self._unix_time_index] - oldest_buffer_unix_time) <= self._cluster_margin_s)]

          logger.info(current_buffer)

          # Need to benchmark that - it is easier to work with numpy array
          cluster_data = np.array(current_buffer, dtype=object)
          # Need to add clustering status
          cluster_data = np.append(cluster_data,
                                      np.zeros((cluster_data.shape[0], 1)).astype(bool),
                                      axis=1)

          logger.info(cluster_data)

          # We run a single iteration of the clustering algorithm around
          # the oldest candidate in the buffer clustering data
          self._create_cluster(cluster_data)

          logger.info(cluster_data)

          clustered = cluster_data[cluster_data[:, -1] == True][:, :-1]

          # NOTE: This is getting a bit out of hand
          # TODO: There must be a better way of doing it
          self._buffer_candidates = list(set(self._buffer_candidates)
                                          - set(map(tuple, clustered)))
          heapq.heapify(self._buffer_candidates)

          logger.info("%d candidates in the TB cluster", clustered.shape[0])

          logger.info("%d candidates left in the buffer queue",
                        len(self._buffer_candidates))

          # Get the highest SNR candidate within the cluster
          clustered = sorted(clustered, key = lambda cand: cand[self._snr_index], 
                                  reverse=True)
          
          trigger_candidate = {x[0]: x[1] for x in zip(self._buffer_keys, clustered[0])}
          trigger_candidate["iso_t"] = Time(trigger_candidate["mjd"], format="mjd").iso
          trigger_candidate["members"] = len(clustered)
          self._trigger(trigger_candidate, True)

          oldest_buffer = None
          current_buffer = None
          clustered = None

      if (len(self._cluster_candidates) > 0):

        current_time = time()
        oldest_cluster = self._cluster_candidates[0]
        oldest_cluster_unix_time = oldest_cluster[self._unix_time_index]

        if ((current_time - oldest_cluster[0]) >= self._cluster_wait_limit):
          logger.debug("Candidates ready for the final clustering")

          # This will be a proper performance killer
          current_cluster = [candidate for candidate in self._cluster_candidates
                            if (abs(candidate[self._unix_time_index] - oldest_cluster_unix_time) <= self._cluster_margin_s)]

          cluster_data = np.array(current_cluster, dtype=object)
          # Need to add clustering status
          cluster_data = np.append(cluster_data,
                                      np.zeros((cluster_data.shape[0], 1)).astype(bool),
                                      axis=1)

          self._create_cluster(cluster_data)

          clustered = cluster_data[cluster_data[:, -1] == True][:, :-1]

          self._cluster_candidates = list(set(self._cluster_candidates)
                                          - set(map(tuple, clustered)))

          heapq.heapify(self._cluster_candidates)

          logger.info("%d candidates in the final cluster", clustered.shape[0])

          logger.info("%d candidates left in the clustering queue",
                        len(self._cluster_candidates))

          for cand in clustered:

            archive_dict = {
              "cand_hash": cand[self._buffer_keys.index("cand_hash")]
            }

            try:
              channel.basic_publish(exchange="post_processing",
                                    routing_key="archiving_" + cand[self._buffer_keys.index("hostname")],
                                    body=dumps(archive_dict))
            except:
              logger.error("Resetting the lost RabbitMQ connection")
              connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
              channel = connection.channel()
              channel.basic_publish(exchange="post_processing",
                                    routing_key="archiving_" + cand[self._buffer_keys.index("hostname")],
                                    body=dumps(archive_dict))

          oldest_cluster = None
          current_cluster = None
          clustered = None

  def _create_cluster(self, cluster_data):

    def __cluster_point(point, cluster_data) -> None:

      # Get points within the neighbourhood of the current point
      # 1. DM within the delta DM of the current point
      # 2. MJD within the width of the current point
      mask_neighbourhood = np.logical_and(np.abs(cluster_data[:, self._dm_index] - point[self._dm_index]) <= point[self._delta_dm_index],
                                          np.abs(cluster_data[:, self._mjd_index] - point[self._mjd_index]) <= (point[self._width_index] / 2.0 / 1000.0 / 86400.0))


      # Combine the neighbourhood mask with candidates that have not yet
      # been included - we don't want to constantly consider points already
      # in the cluster
      mask_full = np.logical_and(np.logical_not(cluster_data[:, -1]), mask_neighbourhood)
      # We need that conversion as the output mask is not actually boolean
      # but of type 'object'
      mask_full = mask_full.astype(bool)

      # Include new points in the cluster
      cluster_data[:, -1] = np.logical_or(cluster_data[:, -1], mask_full)

      if mask_full.any():
        # Now cluster around new points
        point_neighbours = cluster_data[mask_full]
        for new_point in point_neighbours:
          __cluster_point(new_point, cluster_data)
      else:
        return

    __cluster_point(cluster_data[0], cluster_data)

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

    trigger_coord = SkyCoord(ra=cand_data["ra"], dec=cand_data["dec"],
                              unit=(units.hourangle, units.deg), frame="icrs")

    cand_data["ra_deg"] = trigger_coord.ra.deg
    cand_data["dec_deg"] = trigger_coord.dec.deg
    cand_data["gl"] = trigger_coord.galactic.l.deg
    cand_data["gb"] = trigger_coord.galactic.b.deg

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
                          cand_data["ra_deg"],
                          cand_data["dec_deg"],
                          cand_data["gl"],
                          cand_data["gb"],
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
        'ra': cand_data["ra_deg"],
        'dec': cand_data["dec_deg"],
        'gl': cand_data["gl"],
        'gb': cand_data["gb"],
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
      "text": (f"Cluster members: {cand_data['members']}\n"
                f"*Cluster head* (highest SNR member):\n"
                f"MJD: {cand_data['mjd']:.6f}\n"
                f"UTC: {cand_data['iso_t']}\n"
                f"DM: {cand_data['dm']:.2f} pc cm^-3\n"
                f"SNR: {cand_data['snr']:.2f}\n"
                f"Width: {cand_data['width']:.2f} ms\n"
                f"Beam: {cand_data['beam_abs']}\n"
                f"Beam type: {cand_data['beam_type']}\n"
                f"RA: {cand_data['ra']} ({cand_data['ra_deg']:.2f}\u00b0)\n"
                f"DEC: {cand_data['dec']} ({cand_data['dec_deg']:.2f}\u00b0)\n"
                f"gl: {cand_data['gl']:.2f}\u00b0\n"
                f"gb: {cand_data['gb']:.2f}\u00b0\n"
                f"Hostname: {cand_data['hostname']}")
    }

    trigger_message = {
      "attachments": [message]
    }

    trigger_message_json = dumps(trigger_message)
    try:
      req.post(self._slack_hook,
                data=trigger_message_json)
    except:
      logging.error("Could not send the Slack message! "
                    "Is the network connection down?")
    else:
      logging.debug("Slack message sent successfully")