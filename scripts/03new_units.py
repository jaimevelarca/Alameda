#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 11:05:02 2022

@author: jaimesalcedovelarca
"""

import os

import pandas as pd

from pandas import ExcelWriter


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

df01 = CLIENTSUNITS.merge(alameda[['BODEGA','FRENTE', 'FONDO', 'TAMANO', 'NIVEL', 'TIPO','DESCRIPCION','RENT RATE', 'RR M2']],
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
unique = unique[['DistAlameda','BODEGA','TAMANO','RENT RATE','RR M2','min','NoUnits','DESCRIPCION','branch_name']]

multiple01 = df03[df03['NoUnits'] != 1]

MULTUNITS = multiple01.groupby('CLIENT CODE').agg({'DistAlameda':'mean',
                                             'TAMANO':'sum',
                                             'RENT RATE':'sum',
                                             'RR M2':'mean',
                                             'min':'mean',
                                             'NoUnits':'mean'
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
            'NIVEL', 'TIPO', 'DESCRIPCION','RENT RATE', 'RR M2', 'SR M2']
    
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
    CLIENTES = CLIENTES[['BODEGA','TAMANO','RENT RATE']]
    CLIENTES.sort_values(by='TAMANO',ascending=False,inplace=True)
    CLIENTES.reset_index(inplace=True)

    # Load DF of branch available storage units
    BRANCH = dic_freeunits[SUC]
    BRANCH = BRANCH[['BODEGA','TAMANO','RENT RATE']]
    BRANCH.sort_values(by='TAMANO',ascending=False,inplace=True,ignore_index=True)
    
    IX = []
    ACTUAL = []
    UNIT = []
    
    for ROW in range(len(CLIENTES)):
        for ROW2 in range(len(BRANCH)):
                if (CLIENTES.loc[ROW,'TAMANO'] <= BRANCH.loc[ROW2,'TAMANO'] <= CLIENTES.loc[ROW,'TAMANO']*1.25) and (CLIENTES.loc[ROW,'RENT RATE']*.8 <= BRANCH.loc[ROW2,'RENT RATE'] <= CLIENTES.loc[ROW,'RENT RATE']*1.2):
                    IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                    ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                    UNIT.append(BRANCH.loc[ROW2,'BODEGA'])
                    #BRANCH = BRANCH.drop(ROW2,axis=0)
                    #BRANCH.reset_index(inplace=True,drop=True)
                    #break
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
    UNITSDF = UNITSDF[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','RENT RATE']]
    # Clients df
    CLIENTSDF = dic_multclient[SUC].reset_index()
    CLIENTSDF = CLIENTSDF[['CLIENT CODE','BODEGA','TAMANO','DESCRIPCION','RENT RATE']]
    
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
check.drop_duplicates(subset='ClientID',keep='first',inplace=True)

ok = df[df['NewUnit'] != 'Revisar']

clients_new_units_all_options00 = ok.copy()

ok = ok.drop_duplicates(subset='ClientID',keep='first')

clients_new_units_01 = ok.copy()


#%% 5.2 - Options for clients with single units
newunits01 = {}
sucursales = list(dic_singclient.keys())

for SUC in sucursales:
    print(f'Start loop: {SUC}')
    # Load DF of clients near said branch
    CLIENTES = dic_singclient[SUC]
    CLIENTES = CLIENTES[['BODEGA','TAMANO','RENT RATE']]
    CLIENTES.sort_values(by='TAMANO',ascending=True,inplace=True)
    CLIENTES.reset_index(inplace=True)

    # Load DF of branch available storage units
    BRANCH = dic_freeunits2[SUC]
    BRANCH = BRANCH[['BODEGA','TAMANO','RENT RATE']]
    BRANCH.sort_values(by='TAMANO',ascending=True,inplace=True,ignore_index=True)
    
    IX = []
    ACTUAL = []
    UNIT = []
    
    for ROW in range(len(CLIENTES)):
        for ROW2 in range(len(BRANCH)):
                if (CLIENTES.loc[ROW,'TAMANO'] <= BRANCH.loc[ROW2,'TAMANO'] <= CLIENTES.loc[ROW,'TAMANO']*1.3) and (CLIENTES.loc[ROW,'RENT RATE']*.8 <= BRANCH.loc[ROW2,'RENT RATE'] <= CLIENTES.loc[ROW,'RENT RATE']*1.2):
                    IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                    ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                    UNIT.append(BRANCH.loc[ROW2,'BODEGA'])
                    #BRANCH = BRANCH.drop(ROW2,axis=0)
                    #BRANCH.reset_index(inplace=True,drop=True)
                    #break
                else:
                    IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                    ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                    UNIT.append('Revisar')
                    pass
                    # IX.append(CLIENTES.loc[ROW,'CLIENT CODE'])
                    # ACTUAL.append(CLIENTES.loc[ROW,'BODEGA'])
                    # UNIT.append('Revisar')
                    # break

        else:
            continue
        
    newunits01[SUC] = [IX,ACTUAL,UNIT]


#%% Single units df
# =============================================================================

new = pd.DataFrame()

for KEY,VAL in zip(newunits01.keys(),newunits01.values()):
    
    TEMP = pd.DataFrame(VAL).T
    TEMP.columns = ['ClientID','CurrentUnit','NewUnit']
    TEMP['NewBranch'] = KEY
    
    new = new.append(TEMP)

new.drop_duplicates(inplace=True)

#%% Cross validation

df = pd.DataFrame()

for SUC in sucursales:
    # Temp dataframe
    TEMP = new[new['NewBranch'] == SUC]
    
    # Units df
    UNITSDF = dic_freeunits2[SUC]
    UNITSDF = UNITSDF[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','RENT RATE']]
    # Clients df
    CLIENTSDF = dic_singclient[SUC].reset_index()
    CLIENTSDF = CLIENTSDF[['CLIENT CODE','BODEGA','TAMANO','DESCRIPCION','RENT RATE']]
    
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
check.drop_duplicates(subset='ClientID',keep='first',inplace=True)

ok = df[df['NewUnit'] != 'Revisar']

clients_new_units_all_options01 = ok.copy()

ok01 = ok.drop_duplicates(subset='ClientID',keep='first')

clients_new_units_02 = ok01.copy()


# =============================================================================
#%% 6.0 - New approach for clients with a single unit
# =============================================================================

### Dataframe of branches by proximity
branches = pd.DataFrame()

for ID in range(len(distance02)):
    BRANCH = distance02.copy()
    BRANCH = BRANCH.drop(columns=['min','branch_name'],axis=1)
    BRANCH = BRANCH.T

    CLIENT = BRANCH.iloc[:,ID].to_frame()
    COL = CLIENT.columns
    CLIENT = CLIENT.sort_values(by=[COL[0]],ascending=True)
    CLIENT.iloc[:,0] = CLIENT.index
    CLIENT.reset_index(drop=True,inplace=True)
    
    branches = pd.concat([branches,CLIENT], axis=1)
    branches.reset_index(drop=True,inplace=True)
    
### Dictionary of available units
available_allunits = {}

UNITVAILABLE = unitlist[unitlist['ESTATUS'] == 'Available']

COLS = ['SUCURSAL', 'ID BODEGA', 'BODEGA','ESTATUS', 'FRENTE', 'FONDO', 'TAMANO',
        'NIVEL', 'TIPO', 'DESCRIPCION','RENT RATE', 'RR M2', 'SR M2']

UA = UNITVAILABLE[COLS]

NBRANCH = UNITVAILABLE['SUCURSAL'].unique()

for BRANCH in NBRANCH:
    DF = UA[UA['SUCURSAL'] == BRANCH]
    DF.reset_index(drop=True,inplace=True)
    available_allunits[BRANCH] = DF

#%% DF of remaining clients without new units
remainingclients = unique.copy()
remainingclients = remainingclients[['BODEGA','TAMANO','RENT RATE']]

#%% Loop
# Wrangle clients
remainingclients['ClientID'] = remainingclients.index

# Wrangle branches
branches = branches.T
VALS = {'Anzures Polanco':'Anzures','Paseo Interlomas':'Paseo Interloma','Vasco de Quiroga':'Vasco'}
branches.replace(VALS,inplace=True)

# New options for clients dictionary

newunits02 = {}

remainingclientsid = list(remainingclients.index)
aaorder = list(branches.columns)

for ID in remainingclientsid:
    remainingclientsdic = {}
    for ORDER in aaorder:
        BRANCH = branches.loc[ID,ORDER]
        UNITSDF = available_allunits[BRANCH]
        
        CLIENTID = []
        ACTUALUNIT = []
        NEWBRANCH = []
        BRANCHORDER = []
        NEWUNITS = []
        
        for ROW in range(len(UNITSDF)):

            if (remainingclients.loc[ID,'TAMANO'] <= UNITSDF.loc[ROW,'TAMANO'] <= remainingclients.loc[ID,'TAMANO']*1.25) and (remainingclients.loc[ID,'RENT RATE']*.8 <= UNITSDF.loc[ROW,'RENT RATE'] <= remainingclients.loc[ID,'RENT RATE']*1.2):
                CLIENTID.append(remainingclients.loc[ID,'ClientID'])
                ACTUALUNIT.append(remainingclients.loc[ID,'BODEGA'])
                NEWBRANCH.append(UNITSDF.loc[ROW,'SUCURSAL'])
                BRANCHORDER.append(ORDER)
                NEWUNITS.append(UNITSDF.loc[ROW,'BODEGA'])
            else:
                CLIENTID.append(remainingclients.loc[ID,'ClientID'])
                ACTUALUNIT.append(remainingclients.loc[ID,'BODEGA'])
                NEWBRANCH.append('Revisar')
                BRANCHORDER.append(ORDER)
                NEWUNITS.append('Revisar')
        
        remainingclientsdic[BRANCH] = [CLIENTID,ACTUALUNIT,BRANCHORDER,NEWBRANCH,NEWUNITS]
    
    newunits02[ID] = remainingclientsdic

#%% Dictionary to df

new = pd.DataFrame()

for VAL in newunits02.values():
    DF = VAL

    for KEY,VAL in zip(DF.keys(),DF.values()):
        
        TEMP = pd.DataFrame(VAL).T
        TEMP.columns = ['ClientID','CurrentUnit','Proximity','NewBranch','NewUnit']
        
        new = new.append(TEMP)

new = new.sort_values(by='Proximity',ascending=True)

#%% Cross validation
### Clients

clientsdf = unique[['BODEGA','TAMANO','DESCRIPCION','RENT RATE']]
clientsdf.reset_index(inplace=True)

# Clients merge
CLEFT = ['ClientID','CurrentUnit']
CRIGHT = ['CLIENT CODE','BODEGA']

df = pd.merge(new,clientsdf, left_on=CLEFT, right_on=CRIGHT, how='left')

### Units
unitsdf = pd.DataFrame()

for VAL in available_allunits.values():
    unitsdf = unitsdf.append(VAL)

unitsdf = unitsdf[['SUCURSAL','BODEGA','TAMANO','DESCRIPCION','RENT RATE']]

# Units merge
ULEFT = ['NewBranch','NewUnit']
URIGHT = ['SUCURSAL','BODEGA']

df = pd.merge(df, unitsdf, left_on=ULEFT, right_on=URIGHT, how='left')

#%% Check on errors

check = df[df['NewUnit'] == 'Revisar']
check.drop_duplicates(subset='ClientID',keep='first',inplace=True)

ok = df[df['NewUnit'] != 'Revisar']

clients_new_units_all_options02 = ok.copy()

ok02 = ok.drop_duplicates(subset='ClientID',keep='first')

clients_new_units_03 = ok02.copy()

# =============================================================================
#%% 07 - Final dataframe
# =============================================================================
### First option for each client
clients_new_units_01['Proximity'] = '0'

COLS = {'BODEGA_x':'Bodega actual', 'TAMANO_x':'Tamaño actual',
        'DESCRIPCION_x':'Descripción actual', 'RENT RATE_x':'RR actual',
        'SUCURSAL':'Nueva Sucursal', 'BODEGA_y':'Bodega nueva', 'TAMANO_y':'Tamaño nuevo',
        'DESCRIPCION_y':'Descripción nueva', 'RENT RATE_y':'RR Nuevo'}

clients_new_units_01 = clients_new_units_01.rename(columns=COLS)
clients_new_units_03 = clients_new_units_03.rename(columns=COLS)

COLS = ['ClientID','Bodega actual','Nueva Sucursal','Proximity', 'Bodega nueva','Tamaño actual',
        'Tamaño nuevo','Descripción actual','Descripción nueva','RR actual','RR Nuevo']

clients_new_units_01 = clients_new_units_01[COLS]
clients_new_units_03 = clients_new_units_03[COLS]

newunits = pd.concat([clients_new_units_01,clients_new_units_03])

### All options for all clients
clients_new_units_all_options00['Proximity'] = '0'

COLS = {'BODEGA_x':'Bodega actual', 'TAMANO_x':'Tamaño actual',
        'DESCRIPCION_x':'Descripción actual', 'RENT RATE_x':'RR actual',
        'SUCURSAL':'Nueva Sucursal', 'BODEGA_y':'Bodega nueva', 'TAMANO_y':'Tamaño nuevo',
        'DESCRIPCION_y':'Descripción nueva', 'RENT RATE_y':'RR Nuevo'}

clients_new_units_all_options00 = clients_new_units_all_options00.rename(columns=COLS)
clients_new_units_all_options02 = clients_new_units_all_options02.rename(columns=COLS)

COLS = ['ClientID','Bodega actual','Nueva Sucursal','Proximity', 'Bodega nueva','Tamaño actual',
        'Tamaño nuevo','Descripción actual','Descripción nueva','RR actual','RR Nuevo']

clients_new_units_all_options00 = clients_new_units_all_options00[COLS]
clients_new_units_all_options02 = clients_new_units_all_options02[COLS]

newunitsall = pd.concat([clients_new_units_all_options00,clients_new_units_all_options02])

#%% Add contact info
info = clients[['CLIENT CODE','CLIENT NAME','PHONE #1','E-MAIL']]
info = info.drop_duplicates(subset='CLIENT CODE')

newunits = pd.merge(newunits,info,left_on='ClientID',right_on='CLIENT CODE', how='left')
newunits.drop('CLIENT CODE',axis=1,inplace=True)
newunits.reset_index(drop=True,inplace=True)

newunitsall = pd.merge(newunitsall,info,left_on='ClientID',right_on='CLIENT CODE', how='left')
newunitsall.drop('CLIENT CODE',axis=1,inplace=True)
newunitsall.reset_index(drop=True,inplace=True)

#%% Add  message for mail

newunits['Mensaje'] = ''

for IX in range(len(newunits)):
    NOMBRE = newunits.loc[IX,'CLIENT NAME']
    NUEVASUCURSAL = newunits.loc[IX,'Nueva Sucursal']
    TAMANO = newunits.loc[IX,'Tamaño nuevo']
    NEWRR = newunits.loc[IX,'RR Nuevo']
    
    newunits.loc[IX,'Mensaje'] = f'Estimado {NOMBRE},\nPara nosotros en FIBRA Storage es muy importante brindarle todo nuestro apoyo en este proceso en el que nos encontramos con la sucursal de alameda.\
        \nSabemos que el dejar de contar con un espacio que sirve como extensión de tu casa es una situación poco ideal.\
        \nEs por eso que buscamos brindarle opciones para que tengas menos de que preocuparte.\
        \n\
        \nCon base en la información que nos proporcionaste cuando comenzaste a rentar con nosotros, hemos buscado espacios disponibles en otras sucursales que están lo más cercano posible a la dirección que registraste con nosotros.\
        \nEste nuevo espacio que proponemos es lo más parecido a lo que actualmente rentas en la sucursal de Alameda:\
        \n*Se encuentra ubicado en la sucursal de {NUEVASUCURSAL}.\
        \n*Tiene un tamaño de {TAMANO} m2.\
        \n*El nuevo costo de renta sería de {NEWRR}.\
        \n\
        \nSi esta opción no te convence o si quieres ver más opciones, te pedimos que por favor contactes a Nancy, quién estará más que feliz en ayudarte a encontrar el espacio ideal.\
        \n\
        \n*Recuerda que las bodegas estás sujetas a disponibilidad y es probable que algún otro cliente la pueda rentar.'


newunitsall['Mensaje'] = ''

for IX in range(len(newunitsall)):
    NOMBRE = newunitsall.loc[IX,'CLIENT NAME']
    NUEVASUCURSAL = newunitsall.loc[IX,'Nueva Sucursal']
    TAMANO = newunitsall.loc[IX,'Tamaño nuevo']
    NEWRR = newunitsall.loc[IX,'RR Nuevo']
    
    newunitsall.loc[IX,'Mensaje'] = f'Estimado {NOMBRE},\nPara nosotros en FIBRA Storage es muy importante brindarle todo nuestro apoyo en este proceso en el que nos encontramos con la sucursal de alameda.\
        \nSabemos que el dejar de contar con un espacio que sirve como extensión de tu casa es una situación poco ideal.\
        \nEs por eso que buscamos brindarle opciones para que tengas menos de que preocuparte.\
        \n\
        \nCon base en la información que nos proporcionaste cuando comenzaste a rentar con nosotros, hemos buscado espacios disponibles en otras sucursales que están lo más cercano posible a la dirección que registraste con nosotros.\
        \nEste nuevo espacio que proponemos es lo más parecido a lo que actualmente rentas en la sucursal de Alameda:\
        \n*Se encuentra ubicado en la sucursal de {NUEVASUCURSAL}.\
        \n*Tiene un tamaño de {TAMANO} m2.\
        \n*El nuevo costo de renta sería de {NEWRR}.\
        \n\
        \nSi esta opción no te convence o si quieres ver más opciones, te pedimos que por favor contactes a Nancy, quién estará más que feliz en ayudarte a encontrar el espacio ideal.\
        \n\
        \n*Recuerda que las bodegas estás sujetas a disponibilidad y es probable que algún otro cliente la pueda rentar.'

"""
Estimado {NOMBRE},
Para nosotros en FIBRA Storage es muy importante brindarle todo nuestro apoyo en este proceso en el que nos encontramos con la sucursal de Alameda.
Sabemos que el dejar de contar con un espacio adicional con el que ya cuenta, es una situación no deseable. Por lo anterior, queremos ofrecerle opciones para tratar de ayudar en este proceso.
       
Con base en la información que nos proporcionó cuando comenzó a rentar con nosotros, hemos buscado espacios disponibles en otras sucursales que están lo más cercano posible a la dirección registrada. A continuación, le enlistamos opciones lo más parecidas al espacio de {TIPODEBODEGA} que actualmente renta en la sucursal Alameda:
        *Se encuentra ubicado en la sucursal de {NUEVASUCURSAL}.
        *Tiene un tamaño de {TAMANO} m2.
        *La nueva bodega es {NUEVOTIPODEBODEGA}.
        *El nuevo costo de renta sería de {NEWRR}.

        
Si esta opción no le convence o si quiere ver más opciones disponibles, le pedimos que por favor contacte a la gerencia. Cuenta con toda nuestra disposición para ayudarle a encontrar el espacio ideal.
        
        *Las bodegas sugeridas estás sujetas a disponibilidad (actualmente se encuentran libres).

"""

# =============================================================================
#%% 08 - Save to excel
# =============================================================================

finaldic = {'Primera Opcion':newunits,'Todas las opciones':newunitsall}

def save_xls(DictDF, Name):
    """
    Save a dictionary of dataframes to an excel file, 
    with each dataframe as a separate page
    """

    writer = ExcelWriter(RESULTS+Name)
    for KEY in DictDF.keys():
        DictDF[KEY].to_excel(writer, sheet_name=KEY)

    writer.save()

name = input("Ingresa el nombre del archivo que quieres guardar:")

save_xls(finaldic,name+'.xlsx')