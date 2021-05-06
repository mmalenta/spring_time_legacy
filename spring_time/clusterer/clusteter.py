import logging
import queue

from socket import gethostname
from time import sleep
from typing import Dict

from meertrig.voevent import VOEvent
from meertrig.config_helpers import get_config

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

  """

  def __init__(self, configuration: Dict, cand_queue):

    logger.debug("Starting the clusterer...")

    self._candidate_queue = cand_queue
    self._hostname = gethostname()
    self._voevent_defaults = get_config(configuration["voe_defaults"])
    self._voevent = VOEvent(host=configuration["voe_host"],
                            port=configuration["voe_port"])

  def cluster(self) -> None:

    """

    Receives the candidates from the supervisor and runs the clustering.

    Returns:

      None

    """

    while True:

      try:

        candidate = self._candidate_queue.get_nowait()

        logger.debug("Received candidate")
        logger.debug(candidate)

      except queue.Empty:

        logger.debug("No candidate yet...")

      logger.debug("Clusterer waiting for the data...")
      sleep(1)

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