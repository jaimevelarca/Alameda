#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 11:09:07 2022

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
#%% 01 - Load working data
# =============================================================================
clients = pd.read_csv(PROC+'01alameda.csv')
unitlist = pd.read_csv(RAW +'UnitList_US.csv')
distance = pd.read_csv(RESULTS+'distancia_clientes_alameda.csv')

# =============================================================================
#%% 02 - Wrangle data
# =============================================================================
clients = clients.iloc[:,1:]
distance = distance.iloc[:,1:]

CLIENTSUNITS = clients[['CLIENT CODE','UNIT']]

ALAMEDADISTANCE = distance[['CLIENT CODE','Alameda']]
ALAMEDADISTANCE.drop_duplicates(subset=['CLIENT CODE'],inplace=True)

CLIENTSUNITS = CLIENTSUNITS.merge(ALAMEDADISTANCE,right_on='CLIENT CODE',left_on='CLIENT CODE', how='left')

alameda = unitlist[unitlist['SUCURSAL'] == 'Alameda']

df01 = CLIENTSUNITS.merge(alameda[['BODEGA','FRENTE', 'FONDO', 'TAMANO', 'NIVEL', 'TIPO','DESCRIPCION','RR M2']],
                          left_on='UNIT',right_on='BODEGA',how='left')

#%% 02.1 - Get min distance
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

#%% 02.2 - Merge df01 with min distance and branch name
df01.set_index('CLIENT CODE', inplace=True)

df02 = df01.merge(distance02[['min','branch_name']],left_index=True,right_index=True, how='left')

df02['branch_name'].unique()

# df02.to_csv(RESULTS+'01alameda_clientes.csv')

df02['val'] = df02['UNIT'] == df02['BODEGA']
df02['val'].unique()

df02 = df02.drop(labels=['val','UNIT'],axis=1)

# =============================================================================
#%% 03 - Separate clients by 1 unit & multiple units
# =============================================================================

CCOUNT = df02.groupby('CLIENT CODE')['BODEGA'].count()

df03 = df02.merge(CCOUNT.to_frame(), left_index=True,right_index=True,how='left')
df03.rename(columns={'BODEGA_y':'NoUnits','BODEGA_x':'BODEGA','Alameda':'DistAlameda'},inplace=True)

unique = df03[df03['NoUnits'] == 1]
unique = unique[['DistAlameda','BODEGA','TAMANO','RR M2','min','NoUnits','DESCRIPCION','branch_name']]

multiple01 = df03[df03['NoUnits'] != 1]

MULTUNITS = multiple01.groupby('CLIENT CODE').agg({'DistAlameda':'mean',
                                             'TAMANO':'sum',
                                             'RR M2':'mean',
                                             'min':'mean',
                                             'NoUnits':'sum'
                                             })

MUNITS = multiple01.groupby('CLIENT CODE')['BODEGA'].apply(lambda x:' , '.join(map(str,x)))

multiple = MULTUNITS.merge(multiple01[~multiple01.index.duplicated(keep='first')][['DESCRIPCION','branch_name']]
                        ,left_index=True,right_index=True,how='left')

multiple = multiple.merge(MUNITS.to_frame(), left_index=True, right_index=True, how='left')

# =============================================================================
#%% 04 - Create a dictionary to store the clients of different possible branches
# =============================================================================

### Define a function that prepares the data to assign units to each client

def data_prep(ClientsDF,UnitList):
    ### Dictionary of clients new options
    clients_dic = {}
    
    
    NBRANCH = ClientsDF['branch_name'].unique()
    
    for BRANCH in NBRANCH:
        clients_dic[BRANCH] = ClientsDF[ClientsDF['branch_name'] == BRANCH]
    
    ### Dictionary of available units
    available_dic = {}
    
    UNITVAILABLE = UnitList[UnitList['ESTATUS'] == 'Available']
    
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
        try:
            clients_dic[NEW] = clients_dic.pop(OLD)
        except:
            pass
    
    for NEW in N:
        try:
            clients_dic[NEW]['branch_name'] = NEW
        except:
            pass
    
    return clients_dic,available_dic


dic_multclient, dic_freeunits = data_prep(multiple,unitlist)
dic_singclient, dic_freeunits2 = data_prep(unique,unitlist)

# =============================================================================
#%% 05 - New options for each client
# =============================================================================
#%% 05.1 - Options for clients with multiple units
newunits00 = {}
sucursales = list(dic_multclient.keys())

for SUC in sucursales:
    print(f'Start loop: {SUC}')
    # Load DF of clients near said branch
    CLIENTES = dic_multclient[SUC]
    CLIENTES = CLIENTES[['BODEGA','TAMANO']]
    CLIENTES.sort_values(by='TAMANO',ascending=False,inplace=True)
    CLIENTES.reset_index(inplace=True)

    # Load DF of branch available storage units
    BRANCH = dic_freeunits[SUC]
    BRANCH = BRANCH[['BODEGA','TAMANO']]
    BRANCH.sort_values(by='TAMANO',ascending=False,inplace=True,ignore_index=True)
    
    IX = []
    ACTUAL = []
    UNIT = []
    
    
    for ROW in range(len(CLIENTES)):
        for ROW2 in range(len(BRANCH)):
                if CLIENTES.loc[ROW,'TAMANO'] <= BRANCH.loc[ROW2,'TAMANO'] <= CLIENTES.loc[ROW,'TAMANO']*1.5:
                    IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                    ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                    UNIT.append(BRANCH.loc[ROW2,'BODEGA'])
                    BRANCH = BRANCH.drop(ROW2,axis=0)
                    BRANCH.reset_index(inplace=True,drop=True)
                    break
                else:
                    pass
                    # IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                    # ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                    # UNIT.append('Revisar')
                    # break

        else:
            continue
        
    newunits00[SUC] = [IX,ACTUAL,UNIT]

#%% Multiple units df
# =============================================================================

new = pd.DataFrame()

for KEY,VAL in zip(newunits00.keys(),newunits00.values()):
    
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
    UNITSDF = dic_freeunits[SUC]
    UNITSDF = UNITSDF[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','SR M2']]
    # Clients df
    CLIENTSDF = dic_multclient[SUC].reset_index()
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
    TEMP = dic_freeunits[SUC]
    
    available = available.append(TEMP)

available = available[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','SR M2']]

remaining = pd.merge(units,available,left_on=['NewBranch','NewUnit'],right_on=['SUCURSAL','BODEGA'], how='outer')



#%% Options for clients with single unit  - does not work properly
def singleunit_options(ClientsDic,AvailableDic):
    newunits = {}
    sucursales = list(ClientsDic.keys())
    
    for SUC in sucursales:
        print(f'Start loop: {SUC}')
        # Load DF of clients near said branch
        CLIENTES = ClientsDic[SUC]
        CLIENTES = CLIENTES[['BODEGA','TAMANO']]
        CLIENTES.sort_values(by='TAMANO',ascending=True,inplace=True)
        CLIENTES.reset_index(inplace=True)
    
        # Load DF of branch available storage units
        BRANCH = AvailableDic[SUC]
        BRANCH = BRANCH[['BODEGA','TAMANO']]
        BRANCH.sort_values(by='TAMANO',ascending=True,inplace=True,ignore_index=True)
        
        IX = []
        ACTUAL = []
        UNIT = []
        
        
        for ROW in range(len(CLIENTES)):
            print(ROW)
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
        
        return newunits


clientsmultipleunits = singleunit_options(dic_singclient, dic_freeunits2)

#%% For loop replacement
newunits = {}
sucursales = list(dic_singclient.keys())

for SUC in sucursales:
    print(f'Start loop: {SUC}')
    # Load DF of clients near said branch
    CLIENTES = dic_singclient[SUC]
    CLIENTES = CLIENTES[['BODEGA','TAMANO']]
    CLIENTES.sort_values(by='TAMANO',ascending=True,inplace=True)
    CLIENTES.reset_index(inplace=True)

    # Load DF of branch available storage units
    BRANCH = dic_freeunits2[SUC]
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

#%% Create DF
# =============================================================================

new1 = pd.DataFrame()

for KEY,VAL in zip(newunits.keys(),newunits.values()):
    
    TEMP = pd.DataFrame(VAL).T
    TEMP.columns = ['ClientID','CurrentUnit','NewUnit']
    TEMP['NewBranch'] = KEY
    
    new1 = new1.append(TEMP)

#%% Cross validation

df1 = pd.DataFrame()

for SUC in sucursales:
    # Temp dataframe
    TEMP = new1[new1['NewBranch'] == SUC]
    
    # Units df1
    UNITSdf1 = dic_freeunits2[SUC]
    UNITSdf1 = UNITSdf1[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','SR M2']]
    # Clients df1
    CLIENTSdf1 = dic_singclient[SUC].reset_index()
    CLIENTSdf1 = CLIENTSdf1[['CLIENT CODE','BODEGA','TAMANO','DESCRIPCION','RR M2']]
    
    # Clients merge
    CLEFT = ['ClientID','CurrentUnit']
    CRIGHT = ['CLIENT CODE','BODEGA']
    
    TEMP = TEMP.merge(CLIENTSdf1, left_on=CLEFT, right_on=CRIGHT, how='left')
    
    # Units merge
    ULEFT = ['NewBranch','NewUnit']
    URIGHT = ['SUCURSAL','BODEGA']
    
    TEMP = TEMP.merge(UNITSdf1, left_on=ULEFT, right_on=URIGHT, how='left')
    
    df1 = df1.append(TEMP)

#%% Check on errors

check1 = df1[df1['NewUnit'] == 'Revisar']
ok1 = df1[df1['NewUnit'] != 'Revisar']

# Remove assigned units
units1 = df1[['NewUnit','NewBranch']]

available = pd.DataFrame()

for SUC in sucursales:
    TEMP = dic_freeunits2[SUC]
    
    available = available.append(TEMP)

available = available[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','SR M2']]

remaining = pd.merge(units,available,left_on=['NewBranch','NewUnit'],right_on=['SUCURSAL','BODEGA'], how='outer')

# =============================================================================
#%% 06 - Final DF
# =============================================================================
options = pd.DataFrame()

options = options.append(ok)
options = options.append(ok1)

options['ClientID'] == options['CLIENT CODE']

COLS = ['ClientID', 'CurrentUnit','NewBranch','NewUnit','TAMANO_x','TAMANO_y','RR M2','SR M2','DESCRIPCION_x','DESCRIPCION_y']

options2 = options[COLS]


options.to_csv(RESULTS+'opciones_clientes.csv')


