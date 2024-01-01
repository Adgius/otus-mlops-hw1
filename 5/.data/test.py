import sys
import logging
import os
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()
logger.info("Python path: {}".format(sys.executable))
logger.info("Python version: {}".format(sys.version))
logger.info(os.environ)
logger.info("Env from cmd: {}".format(subprocess.run(["printenv"])))