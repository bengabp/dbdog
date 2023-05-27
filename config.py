import os
from dotenv import load_dotenv
import logging
import sys
from colorlog import ColoredFormatter

DIR_PATH = os.path.dirname(os.path.realpath(__file__))

load_dotenv(os.path.join(DIR_PATH,".env"))
