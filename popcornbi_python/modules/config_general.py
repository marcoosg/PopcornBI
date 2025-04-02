import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
from tmdbv3api import TMDb, Movie
from concurrent.futures import ThreadPoolExecutor
from iso639 import languages

import logging

log_dir = "logs"
log_file = os.path.join(log_dir, "logs.log")
os.makedirs(log_dir, exist_ok=True)

if not os.path.exists(log_file):
    with open(log_file, "w"):  
        pass

logging.basicConfig(
    filename=log_file,
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)