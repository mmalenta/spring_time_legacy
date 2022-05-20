import logging
import pika

import argparse as ap
import multiprocessing as mp

from supervisor.supervisor import Supervisor

logger = logging.getLogger()

class ColouredFormatter(logging.Formatter):

  """
  
  Provides a custom logger formatter.
  
  """

  custom_format = "[%(asctime)s] [%(process)d %(processName)s] " \
                  "[\033[1;{0}m%(levelname)s\033[0m] [%(module)s] %(message)s"

  def format(self, record):

    colours = {
      logging.DEBUG: 30,
      logging.INFO: 32,
      15: 36,
      logging.WARNING: 33,
      logging.ERROR: 31
    }

    colour_number = colours.get(record.levelno)
    return logging.Formatter(self.custom_format.format(colour_number), datefmt="%a %Y-%m-%d %H:%M:%S").format(record)

class ClusterFilter():

  """

  Provides a custom logger filter for cluster messages.

  """

  def __init__(self, level=15):
    self._level = level

  def filter(self, record) -> bool:

    """
    
    Checks whether the level of the current record is the same as
    the cluster filte level.

    Parameters:

      record

    Returns:

      : bool
    
    """

    return record.levelno == self._level

def main():

  parser = ap.ArgumentParser(description="MeerTRAP supervisor for the Spring post-processing pipeline",
                              usage="%(prog)s <options>",
                              epilog="For any bugs, please start an issue at https://github.com/mmalenta/spring_time")

  general_group = parser.add_argument_group("General")

  general_group.add_argument("-l", "--log",
                      help="Log level",
                      required=False,
                      type=str,
                      choices=["debug", "info", "warn"],
                      default="debug")
  
  voe_group = parser.add_argument_group("VOEvents")

  voe_group.add_argument("--voedef",
                      help="Path to file with the default VOEvent \
                            trigger values",
                      required=True,
                      type=str)

  voe_group.add_argument("--voehost",
                          help="Name or the IP of the VOEvent broker",
                          required=False,
                          type=str,
                          default="localhost")
                  
  voe_group.add_argument("--voeport",
                          help="Port the VOEvent broker listens on",
                          required=False,
                          type=int,
                          default=8090)

  arguments = parser.parse_args()
  logging.addLevelName(15, "CANDIDATE")
  logger.setLevel(getattr(logging, arguments.log.upper()))
  clhandler = logging.StreamHandler()
  clhandler.setLevel(getattr(logging, arguments.log.upper()))
  clhandler.setFormatter(ColouredFormatter())
  logger.addHandler(clhandler)
  logger.debug("Logging set up")

  # TODO: Add path there - we need to pass it
  fl_handler = logging.FileHandler()
  fl_formatter = logging.Formatter("%(asctime)s: %(message)s",
                                datefmt="%a %Y-%m-%d %H:%M:%S")
  fl_handler.setFormatter(fl_formatter)
  fl_handler.addFilter(ClusterFilter())
  logger.addHandler(fl_handler)

  configuration = {
    "voe_defaults": arguments.voedef,
    "voe_host": arguments.voehost,
    "voe_port": arguments.voeport
  }

  supervisor = Supervisor(configuration)
  supervisor.supervise()

if __name__ == "__main__":
  main()