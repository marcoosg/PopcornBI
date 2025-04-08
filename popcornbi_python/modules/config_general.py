import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
from tmdbv3api import TMDb, Movie
from concurrent.futures import ThreadPoolExecutor
from iso639 import languages

import logging
import sys

log_dir = "logs"
log_file = os.path.join(log_dir, "logs.log")
os.makedirs(log_dir, exist_ok=True)

if not os.path.exists(log_file):
    with open(log_file, "w"):  
        pass

# Set up basic logging to file
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Get logger and add console handler (if not already added)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Prevent duplicate handlers if reloaded
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)