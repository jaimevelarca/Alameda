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

# =============================================================================
#%% New options for each client
# =============================================================================

options_dic = {}
"""
for suc in sucursales:
    clientid = clientes[suc][id]
    
    for id in clientesid:
        list = []
        for Unit in disponible[suc]:
            if id[unitm2] <= Unit <= id[unitm2]*(1.1):
                list.append(Unit)
        vacio[id] = list
"""
sucursales = list(clients_dic.keys())

#%% Loop: people with 1 rental unit

for SUC in sucursales:
    # Load DF of clients near said branch
    CLIENTES = clients_dic[SUC]
    # Get clients ID
    CLIENTESID = list(CLIENTES.index)
    # Load DF of branch available storage units
    BRANCH = available_dic[SUC]
    
    for ID in CLIENTESID:
        options_dic[ID] = {}
        SIMILARUNITS = []
        
        UNIT = CLIENTES.loc[ID,'TAMANO']
        UCODE = CLIENTES.loc[ID,'BODEGA']
        
        try:
            for ROW in range(len(BRANCH)):
                SIZE = BRANCH.columns.get_loc('TAMANO')
                UNITID = BRANCH.columns.get_loc('BODEGA')
                
                UNITSIZE = BRANCH.iloc[ROW,SIZE]
                
                if UNIT <= UNITSIZE <= UNIT*1.125:
                    BODEGA = BRANCH.iloc[ROW,UNITID]
                    
                    SIMILARUNITS.append(BODEGA)
                else:
                    pass
                options_dic[ID][UCODE] = SIMILARUNITS
        except:
            pass

# =============================================================================
#%% Create a df for the options
# =============================================================================
### DF for clients with only 1 rental unit
optionsdf = pd.DataFrame(options_dic)
optionsdf = optionsdf.replace(list(),'')
optionsdf = optionsdf.dropna(axis=1,how='all').dropna(axis=0,how='all')

options01 = pd.DataFrame()

for ROW in range(len(optionsdf)):
    
    CLEAN = pd.DataFrame(optionsdf.iloc[ROW,:]).dropna(axis=1,how='all').dropna(axis=0,how='all')
    ID = CLEAN.index
    UNIT = CLEAN.columns
    
    options01.loc[ROW,'ClientID'] = str(ID[0])
    options01.loc[ROW,'Unit'] = UNIT[0]
    
    OPTIONS = CLEAN.iloc[0,0]
    
    for COL in range(len(OPTIONS)):
        options01.loc[ROW,f'Option{COL+1}'] = OPTIONS[COL]

# Clients that does not have a potential option in the first try
options02 = options01[options01['Option1'].isna()]
# Clients that have options in the first try
options01 = options01[options01['Option1'].notna()]
# Adding the branch that is nearer to their address
options01['New Branch'] = ''

ID = options01.loc[:,'ClientID'].tolist()

options01.set_index('ClientID',inplace=True)

for IX in ID:
    options01.loc[IX,'New Branch'] = df02.loc[IX,'branch_name']

# Saving the df01
# options01.to_csv(RESULTS+'opciones/options01.csv')

#%% Final df with only 1 option

df = options01.copy()
# Create a list of the branches to loop through it
branches = df['New Branch'].unique().tolist()

# Handle indexes
df.reset_index(inplace=True)
df.set_index(['New Branch','ClientID','Unit'], inplace=True)

dicvf = {}
dictaken = {}
dfvf = pd.DataFrame()

for BRANCH in branches:
    # subset by branch in a temporary df
    TEMP = df.loc[BRANCH]
    # Create an empty df
    NEW = pd.DataFrame()
    # Assign the same index to the new df
    NEW.index = TEMP.index
    # Create a col for the new unit
    NEW['Sugerencia'] = ''
    NEW['Branch Name'] = BRANCH
    
    TAKEN = []
    
    #Loop through the TEMP df
    for ROW in range(len(TEMP)):
        # temporary list
        LIST = TEMP.iloc[ROW,:].tolist()
        LIST = [x for x in LIST if str(x) != 'nan']
        
        try:
            if ROW == 0:
                NEW.iloc[ROW,0] = LIST[0]
                TAKEN.append(LIST[0])
            else:
                for UT in TAKEN:
                    if UT in LIST:
                        LIST.remove(UT)
                    else:
                        pass
                try:
                    NEW.iloc[ROW,0] = LIST[0]
                    TAKEN.append(LIST[0])
                except:
                    pass
        except:
            pass
        
    dicvf[BRANCH] = NEW
    dictaken[BRANCH] = TAKEN
    dfvf = dfvf.append(NEW)

# dfvf.to_csv(RESULTS+'opciones/01options.csv')

#%% Test loop
# Loop through clients_dic & available_dic to obtain similar units

#empty = []

emptydic = {}

for SUC in sucursales:
    # Load DF of clients near said branch
    CLIENTES = clients_dic[SUC]
    # Get clients ID
    CLIENTESID = list(CLIENTES.index)
    # Load DF of branch available storage units
    BRANCH = available_dic[SUC]
    
    for ID in CLIENTESID:
        emptydic[ID] = {}
        SIMILARUNITS = []
        
        UNIT = CLIENTES.loc[ID,'TAMANO']
        UCODE = CLIENTES.loc[ID,'BODEGA']
        
        try:
            for ROW in range(len(BRANCH)):
                SIZE = BRANCH.columns.get_loc('TAMANO')
                UNITID = BRANCH.columns.get_loc('BODEGA')
                
                UNITSIZE = BRANCH.iloc[ROW,SIZE]
                
                if UNIT <= UNITSIZE <= UNIT*1.125:
                    BODEGA = BRANCH.iloc[ROW,UNITID]
                    
                    SIMILARUNITS.append(BODEGA)
                else:
                    pass
                emptydic[ID][UCODE] = SIMILARUNITS
        except:
            pass

#%% Search algorithm

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
    UNIT = []
    
    for ROW in range(len(CLIENTES)):
        for ROW2 in range(len(BRANCH)):
            if CLIENTES.loc[ROW,'TAMANO'] <= BRANCH.loc[ROW2,'TAMANO']:
                IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                UNIT.append(BRANCH.loc[ROW2,'BODEGA'])
                BRANCH = BRANCH.drop(ROW2,axis=0)
                BRANCH.reset_index(inplace=True,drop=True)
                break
            else:
                IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                UNIT.append('Revisar')
                break
        else:
            continue
        
    newunits[SUC] = [IX,UNIT]

#%% Create DF

new = pd.DataFrame()

for KEY,VAL in zip(newunits.keys(),newunits.values()):
    
    TEMP = pd.DataFrame(VAL).T
    TEMP.columns = ['ClientID','NewUnit']
    TEMP['NewBranch'] = KEY
    
    new = new.append(TEMP)

check = new[new['NewUnit'] == 'Revisar']
