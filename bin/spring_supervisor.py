import logging
import pika

import argparse as ap
import multiprocessing as mp

from supervisor.supervisor import Supervisor

logger = logging.getLogger()

class ColouredFormatter(logging.Formatter):

  custom_format = "[%(asctime)s] [%(process)d %(processName)s] [\033[1;{0}m%(levelname)s\033[0m] [%(module)s] %(message)s"

  def format(self, record):

    colours = {
      logging.DEBUG: 30,
      logging.INFO: 32,
      logging.WARNING: 33,
      logging.ERROR: 31
    }

    colour_number = colours.get(record.levelno)
    return logging.Formatter(self.custom_format.format(colour_number), datefmt="%a %Y-%m-%d %H:%M:%S").format(record)

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
  logger.setLevel(getattr(logging, arguments.log.upper()))
  handler = logging.StreamHandler()
  handler.setLevel(getattr(logging, arguments.log.upper()))
  handler.setFormatter(ColouredFormatter())
  logger.addHandler(handler)
  logger.debug("Logging set up")

  configuration = {
    "voe_defaults": arguments.voedef,
    "voe_host": arguments.voehost,
    "voe_port": arguments.voeport
  }

  supervisor = Supervisor(configuration)
  supervisor.supervise()

if __name__ == "__main__":
  main()