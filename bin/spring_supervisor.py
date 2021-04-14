import logging
import pika

import argparse as ap
import multiprocessing as mp

from supervisor.supervisor import Supervisor

logger = logging.getLogger()

def main():

  parser = ap.ArgumentParser(description="MeerTRAP supervisor for the Spring post-processing pipeline",
                              usage="%(prog)s <options>",
                              epilog="For any bugs, please start an issue at https://github.com/mmalenta/spring_time")
                            
  parser.add_argument("-l", "--log",
                      help="Log level",
                      required=False,
                      type=str,
                      choices=["debug", "info", "warn"],
                      default="debug")

  arguments = parser.parse_args()
  logger.setLevel(getattr(logging, arguments.log.upper()))
  handler = logging.StreamHandler()
  formatter = logging.Formatter("%(asctime)s, %(levelname)s: %(message)s",
                                datefmt="%a %Y-%m-%d %H:%M:%S")
  handler.setLevel(getattr(logging, arguments.log.upper()))
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  logger.debug("Logging set up")

  supervisor = Supervisor({})
  supervisor.supervise()

if __name__ == "__main__":
  main()