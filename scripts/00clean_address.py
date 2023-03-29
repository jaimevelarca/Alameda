# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os

import pandas as pd
import geopandas as gpd
import numpy as np


# Set display option to remove scientific notation and restriction on display
pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Set working directory
PATH = '/Users/jaimesalcedovelarca/Library/CloudStorage/OneDrive-Ustorage/FIBRA Storage/Ops/Alameda/'
RAW = PATH + 'data/raw/'
PROC = PATH + 'data/proc/'
RESULTS = PATH + 'results/'

os.chdir(PATH)

# =============================================================================
#%% Load working data
# =============================================================================
# Excel
df = pd.read_csv(RAW+'alamedadatos.csv')
