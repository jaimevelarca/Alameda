#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 04:06:17 2022

@author: jaimesalcedovelarca
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
clients = pd.read_csv(PROC+'01alameda.csv')
unitlist = pd.read_csv(RAW +'UnitList_US.csv')
distance = pd.read_csv(RESULTS+'distancia_clientes_alameda.csv')

#%% Wrangle data
clients = clients.iloc[:,1:]
distance = distance.iloc[:,1:]

CLIENTSUNITS = clients[['CLIENT CODE','UNIT']]

ALAMEDADISTANCE = distance[['CLIENT CODE','Alameda']]
ALAMEDADISTANCE.drop_duplicates(subset=['CLIENT CODE'],inplace=True)

CLIENTSUNITS = CLIENTSUNITS.merge(ALAMEDADISTANCE,right_on='CLIENT CODE',left_on='CLIENT CODE', how='left')

alameda = unitlist[unitlist['SUCURSAL'] == 'Alameda']

df01 = CLIENTSUNITS.merge(alameda[['BODEGA','FRENTE', 'FONDO', 'TAMANO', 'NIVEL', 'TIPO','DESCRIPCION','RR M2']],
                          left_on='UNIT',right_on='BODEGA',how='left')

#%% Get min distance
distance02 = distance.drop(labels=['coordinates','Alameda','Viaducto','Circuito','Churubusco'],axis=1)
distance02.drop_duplicates('CLIENT CODE',inplace=True)

distance02.set_index('CLIENT CODE', inplace=True)

distance02['min'] = ''

for ROW in range(len(distance02)):
    distance02.iloc[ROW,-1] = distance02.iloc[ROW,:-1].min()

distance02['min'] = pd.to_numeric(distance02['min'])

distance02.info()

### Create temporary df

DISTANCETEMP = distance02.copy()
DISTANCETEMP = DISTANCETEMP.drop(labels=['min'],axis=1)

distance02['branch_name'] = ''

for ROW in DISTANCETEMP.index:
    try:
        BRANCH = DISTANCETEMP.where(DISTANCETEMP == distance02.loc[ROW,'min']).dropna(how='all').dropna(axis=1).columns[0]
        distance02.loc[ROW,'branch_name'] = BRANCH
    except:
        pass

#%% Merge df01 with min distance and branch name
df01.set_index('CLIENT CODE', inplace=True)

df02 = df01.merge(distance02[['min','branch_name']],left_index=True,right_index=True, how='left')

df02['branch_name'].unique()

# df02.to_csv(RESULTS+'01alameda_clientes.csv')

df02['val'] = df02['UNIT'] == df02['BODEGA']
df02['val'].unique()

df02 = df02.drop(labels=['val','UNIT'],axis=1)

# =============================================================================
#%% Create a dictionary to store the clients of different possible branches
# =============================================================================

### Dictionary of clients new options
clients_dic = {}

NBRANCH = df02['branch_name'].unique()

for BRANCH in NBRANCH:
    clients_dic[BRANCH] = df02[df02['branch_name'] == BRANCH]

### Dictionary of available units
available_dic = {}

UNITVAILABLE = unitlist[unitlist['ESTATUS'] == 'Available']

COLS = ['SUCURSAL', 'ID BODEGA', 'BODEGA','ESTATUS', 'FRENTE', 'FONDO', 'TAMANO',
        'NIVEL', 'TIPO', 'DESCRIPCION','RR M2', 'SR M2']

UA = UNITVAILABLE[COLS]

# Replace elements in array
O = ['Anzures Polanco','Paseo Interlomas','Vasco de Quiroga']
N = ['Anzures','Paseo Interloma','Vasco']

for OLD, NEW in zip(O,N):
    NBRANCH[NBRANCH==OLD] = NEW

# Fill dictioanry with data

for BRANCH in NBRANCH:
    available_dic[BRANCH] = UA[UA['SUCURSAL'] == BRANCH]

# Rename dictionary keys to match
for OLD,NEW in zip(O,N):
    clients_dic[NEW] = clients_dic.pop(OLD)

for NEW in N:
    clients_dic[NEW]['branch_name'] = NEW

# =============================================================================
#%% New options for each client
# =============================================================================

newunits = {}
sucursales = list(clients_dic.keys())

for SUC in sucursales:
    print(f'Start loop: {SUC}')
    # Load DF of clients near said branch
    CLIENTES = clients_dic[SUC]
    CLIENTES = CLIENTES[['BODEGA','TAMANO']]
    CLIENTES.sort_values(by='TAMANO',ascending=True,inplace=True)
    CLIENTES.reset_index(inplace=True)

    # Load DF of branch available storage units
    BRANCH = available_dic[SUC]
    BRANCH = BRANCH[['BODEGA','TAMANO']]
    BRANCH.sort_values(by='TAMANO',ascending=True,inplace=True,ignore_index=True)
    
    IX = []
    ACTUAL = []
    UNIT = []
    
    
    for ROW in range(len(CLIENTES)):
        for ROW2 in range(len(BRANCH)):
            if CLIENTES.loc[ROW,'TAMANO'] <= BRANCH.loc[ROW2,'TAMANO']:
                IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                UNIT.append(BRANCH.loc[ROW2,'BODEGA'])
                BRANCH = BRANCH.drop(ROW2,axis=0)
                BRANCH.reset_index(inplace=True,drop=True)
                break
            else:
                IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                UNIT.append('Revisar')
                break
        else:
            continue
        
    newunits[SUC] = [IX,ACTUAL,UNIT]

# =============================================================================
#%% Create DF
# =============================================================================

new = pd.DataFrame()

for KEY,VAL in zip(newunits.keys(),newunits.values()):
    
    TEMP = pd.DataFrame(VAL).T
    TEMP.columns = ['ClientID','CurrentUnit','NewUnit']
    TEMP['NewBranch'] = KEY
    
    new = new.append(TEMP)

#%% Cross validation

df = pd.DataFrame()

for SUC in sucursales:
    # Temp dataframe
    TEMP = new[new['NewBranch'] == SUC]
    
    # Units df
    UNITSDF = available_dic[SUC]
    UNITSDF = UNITSDF[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','SR M2']]
    # Clients df
    CLIENTSDF = clients_dic[SUC].reset_index()
    CLIENTSDF = CLIENTSDF[['CLIENT CODE','BODEGA','TAMANO','DESCRIPCION','RR M2']]
    
    # Clients merge
    CLEFT = ['ClientID','CurrentUnit']
    CRIGHT = ['CLIENT CODE','BODEGA']
    
    TEMP = TEMP.merge(CLIENTSDF, left_on=CLEFT, right_on=CRIGHT, how='left')
    
    # Units merge
    ULEFT = ['NewBranch','NewUnit']
    URIGHT = ['SUCURSAL','BODEGA']
    
    TEMP = TEMP.merge(UNITSDF, left_on=ULEFT, right_on=URIGHT, how='left')
    
    df = df.append(TEMP)

#%% Check on errors

check = df[df['NewUnit'] == 'Revisar']
ok = df[df['NewUnit'] != 'Revisar']

# Remove assigned units
units = df[['NewUnit','NewBranch']]

available = pd.DataFrame()

for SUC in sucursales:
    TEMP = available_dic[SUC]
    
    available = available.append(TEMP)

available = available[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','SR M2']]

remaining = pd.merge(units,available,left_on=['NewBranch','NewUnit'],right_on=['SUCURSAL','BODEGA'], how='outer')
