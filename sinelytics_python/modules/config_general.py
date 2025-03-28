import os
import pandas as pd
import matplotlib.pyplot as plt
import json
from tmdbv3api import TMDb, Movie
from concurrent.futures import ThreadPoolExecutor
from iso639 import languages
