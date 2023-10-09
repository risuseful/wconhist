# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 14:07:26 2023

Purpose: Generate WCONHIST data

@author: akmalaulia
"""

# ---------------------------------------
#
# Library calls
#
# ---------------------------------------
import pandas as pd
import datetime
from prophet import Prophet
import pyodbc
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.pyplot import figure
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.neural_network import MLPRegressor


# ------------------------------------------
#
# load data for Bumi Shallow
# 
# ------------------------------------------
query = "SELECT [Well], cast([Timestamp] as Date) as Date, avg([GAS])*1000 as Qg, avg([WHP])*14.5 as P"
query = query + " FROM [DataMining].[dbo].[ExaTrend]"
query = query + " where Timestamp >= '2022-02-28' and left(Well,2) like 'A%' and Well NOT IN ('A-21', 'A-22', 'A-15', 'A-16') "
query = query + " group by Well, cast(Timestamp as Date) order by cast(Timestamp as Date)"

# list online wells
conn = pyodbc.connect(driver='SQL Server', server='**-*****-**', user='***', password='****', database='******')
cursor = conn.cursor()
bm = pd.read_sql_query(query, conn)

# Note:  'bm' looks like this,
#
# bm
# Out[115]: 
#          Well       Date            Qg           P
# 0      A-01 2022-02-28  1000  100
# 1      A-06 2022-02-28  1000  100
# 2      A-07 2022-02-28  1000  100
# 3      A-12 2022-02-28  1000  100
# 4      A-18 2022-02-28  1000  100
#       ...        ...           ...         ...
# 13331  A-10 2023-04-05  3000  200
# 13332  A-17 2023-04-05  3000  200
# 13333  D-01 2023-04-05  3000  200
# 13334  D-04 2023-04-05  3000  200
# 13335  D-07 2023-04-05  3000  200

# [13336 rows x 4 columns]

# format the date
bm['Date'] = pd.to_datetime(bm['Date'], format="%Y-%m-%d") # format the date

# get unique dates, and store in an array
bm_date = bm['Date'].unique()
bm_date = pd.to_datetime(bm_date, format="%Y-%m-%d") # format the date





with open('wconhist_bm.txt', 'w') as f:
    for i, day in enumerate(bm_date):
        
        # filter data for this 'day'
        bm_sub = bm[bm['Date']==day]
        bm_sub = bm_sub.sort_values(by=['Well'])
        
        # write date
        f.write('\n')
        f.write('DATES \n')
        s = str(day.day) + ' ' + day.strftime("%b") + ' ' + str(day.year) + ' /' + ' \n'
        f.write(s)
        f.write('/ \n \n')
        f.write('WCONHIST \n')
        
        # write WCONHIST data
        # note: 
        # bm_sub['Well'].iloc[1]
        # Out[85]: 'BMA-03'
        
        for j in range(len(bm_sub)):

            # well name
            f.write("'" + bm_sub['Well'].iloc[j] + "'" + ' ')
            
            # gas rate
            Qg = round(bm_sub['Qg'].iloc[j], 0)
            if Qg<0.01:
                well_stat = 'STOP'
            else:
                well_stat = 'OPEN'
            f.write(well_stat + ' GRAT 2* ' + str(Qg) + ' ')
        
            # well head pressure
            WHP = round(bm_sub['P'].iloc[j],2)
            f.write('2* ' + str(WHP) + ' 1* / \n')
        
        # end text
        f.write('/ \n')
        
        
# Note: sample results in 'wconhist_bm.txt' - not shown in GitHub
#
# DATES 
# 2 Mar 2022 / 
# / 
 
# WCONHIST 
# 'WL-01' STOP GRAT 2* 0.0 2* 131.08 1* / 
# 'WL-05' STOP GRAT 2* 0.0 2* 217.91 1* / 
# 'WL-12' OPEN GRAT 2* 3000.0 2* 50.21 1* / 
# / 
