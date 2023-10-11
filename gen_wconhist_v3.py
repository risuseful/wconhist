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

# old query: 
#
# query = "SELECT [Well], cast([Timestamp] as Date) as Date, avg([GAS])*1000 as Qg, avg([WHP])*14.5 as P"
# query = query + " FROM [DataMining].[dbo].[ExaTrend]"
# query = query + " where Timestamp >= '2008-01-01' and left(Well,2) like 'BM%' and Well NOT IN ('BMA-21', 'BMA-22', 'BMA-15', 'BMA-16') "
# query = query + " group by Well, cast(Timestamp as Date) order by cast(Timestamp as Date)"

# new query:
# 
# SELECT
# 	t1.Well,
# 	t1.Date,
# 	isnull(t1.RawGrossGas*1000,0) as Qg,
# 	isnull(t1.WHP, 0) as WHP,
# 	isnull(t2.WaterRate,0) as Qw
# FROM [OFM2012].[dbo].[vExaDailyTrend] t1
# LEFT JOIN [OFM2012].[dbo].[vWater_DualDp_Well] t2
# ON
# 	t1.Date = t2.Date
# 	and
# 	t1.Well = t2.Well
# where 
# 	t1.Date > '2008-01-01'
# 	and
# 	left(t1.Well,2) like 'BM%'
# 	and
# 	t1.Well NOT IN ('BMA-21', 'BMA-22', 'BMA-15', 'BMA-16')  
# order by Date

query = "SELECT t1.Well, t1.Date, isnull(t1.RawGrossGas*1000,0) as Qg, isnull(t1.WHP*14.5, 0) as WHP, isnull(t2.WaterRate,0) as Qw"
query = query + " FROM [OFM2012].[dbo].[vExaDailyTrend] t1 LEFT JOIN [OFM2012].[dbo].[vWater_DualDp_Well] t2 ON t1.Date = t2.Date and t1.Well = t2.Well"
query = query + " where t1.Date > '2008-01-01' and left(t1.Well,2) like 'BM%' "
query = query + " 	and t1.Well NOT IN ('BMA-21', 'BMA-22', 'BMA-15', 'BMA-16', 'BMD-05', 'BMD-09', 'BMD-10') order by Date"

# list online wells
conn = pyodbc.connect(driver='SQL Server', server='hq-prismdb-02', user='akmal', password='akmal', database='Datamining')
cursor = conn.cursor()
bm = pd.read_sql_query(query, conn)

# Note:  'bm' looks like this,
#
# bm
# Out[115]: 
#          Well       Date            Qg           P
# 0      BMA-01 2022-02-28  0.000000e+00  159.872853
# 1      BMA-06 2022-02-28  1.869133e+01  321.199107
# 2      BMA-07 2022-02-28  0.000000e+00   95.897170
# 3      BMA-12 2022-02-28  1.155104e+04  240.444854
# 4      BMA-18 2022-02-28  6.543277e+03  227.103766
#       ...        ...           ...         ...
# 13331  BMA-10 2023-04-05  4.517239e+03  262.460475
# 13332  BMA-17 2023-04-05  1.103590e-15  263.105430
# 13333  BMD-01 2023-04-05  3.522043e+03  326.395003
# 13334  BMD-04 2023-04-05  5.082489e+03  340.170014
# 13335  BMD-07 2023-04-05  8.607699e-25  327.409971

# [13336 rows x 4 columns]

# format the date
bm['Date'] = pd.to_datetime(bm['Date'], format="%Y-%m-%d") # format the date

# get unique dates, and store in an array
bm_date = bm['Date'].unique()
bm_date = pd.to_datetime(bm_date, format="%Y-%m-%d") # format the date




# read well vs VFP table index
dvfp = pd.read_csv('well_vfp_index.csv')


with open('wconhist_bm2.txt', 'w') as f:
    for i, day in enumerate(bm_date):
        
        # filter data for i^th 'day' in this loop
        bm_sub = bm[bm['Date']==day]
        bm_sub = bm_sub.sort_values(by=['Well'])
        
        # write date
        f.write('\n')
        f.write('DATES \n')
        s = str(day.day) + ' ' + day.strftime("%b") + ' ' + str(day.year) + ' /' + ' \n' # day.day gives the day of date, daystrftime("%b") give name of month, eg. 'Feb'.
        f.write(s)
        f.write('/ \n \n')
        f.write('WCONHIST \n')
        
        # write WCONHIST data
        # note: 
        # bm_sub['Well'].iloc[1]
        # Out[85]: 'BMA-03'
        
        for j in range(len(bm_sub)):

            # well name
            wnam = bm_sub['Well'].iloc[j]
            f.write("'" + wnam + "'" + ' ')
            
            
            # # gas rate
            # Qg = round(bm_sub['Qg'].iloc[j], 0)
            # if Qg<0.01:
            #     well_stat = 'STOP'
            # else:
            #     well_stat = 'OPEN'
            # f.write(well_stat + ' GRAT 2* ' + str(Qg) + ' ')
            
            
            
            # water rate and gas rate
            Qw = round(bm_sub['Qw'].iloc[j], 0) # water rate
            Qg = round(bm_sub['Qg'].iloc[j], 0) # gas rate
            if Qg<0.01:
                well_stat = 'STOP'
            else:
                well_stat = 'OPEN'
            f.write(well_stat + ' GRAT * ' + str(Qw) + ' ' + str(Qg) + ' ')
        
        
        
            # VFP table
            n_vfp = dvfp[dvfp['Well'] == wnam]['VFP'] # get VFP table number
            f.write(str(np.array(n_vfp)[0]) + ' ') # write this number into WCONHIST. Index [0] to obtain "clean" number
        
            # well head pressure
            WHP = round(bm_sub['WHP'].iloc[j],2)
            f.write('* ' + str(WHP) + ' 1* / \n')
            
            
        
        # end text
        f.write('/ \n') # end this WCONHIST section
        
        
# Note: sample results in 'wconhist_bm.txt'
#
# DATES 
# 2 Mar 2022 / 
# / 
 
# WCONHIST 
# 'BMA-01' STOP GRAT 2* 0.0 2* 165.08 1* / 
# 'BMA-02' STOP GRAT 2* 0.0 2* 222.91 1* / 
# 'BMA-03' OPEN GRAT 2* 2916.0 2* 231.21 1* / 
# 'BMA-04' OPEN GRAT 2* 7207.0 2* 228.28 1* / 
# 'BMA-05' OPEN GRAT 2* 8155.0 2* 241.66 1* / 
# 'BMA-06' OPEN GRAT 2* 29.0 2* 321.0 1* / 
# 'BMA-07' STOP GRAT 2* 0.0 2* 95.73 1* / 
# 'BMA-08' OPEN GRAT 2* 10.0 2* 423.54 1* / 
# 'BMA-09' STOP GRAT 2* 0.0 2* 195.22 1* / 
# 'BMA-10' OPEN GRAT 2* 6597.0 2* 253.07 1* / 
# 'BMA-11' OPEN GRAT 2* 5857.0 2* 245.27 1* / 
# 'BMA-12' OPEN GRAT 2* 11501.0 2* 241.85 1* / 
# 'BMA-13' OPEN GRAT 2* 7694.0 2* 241.25 1* / 
# 'BMA-14' OPEN GRAT 2* 5756.0 2* 256.97 1* / 
# 'BMA-17' STOP GRAT 2* 0.0 2* 470.92 1* / 
# 'BMA-18' OPEN GRAT 2* 6497.0 2* 228.65 1* / 
# 'BMA-19' OPEN GRAT 2* 4439.0 2* 239.42 1* / 
# 'BMA-20' OPEN GRAT 2* 12206.0 2* 235.62 1* / 
# 'BMB-01' OPEN GRAT 2* 3967.0 2* 254.33 1* / 
# 'BMB-02' OPEN GRAT 2* 5942.0 2* 253.34 1* / 
# 'BMB-03' OPEN GRAT 2* 56.0 2* 188.58 1* / 
# 'BMB-04' OPEN GRAT 2* 56.0 2* 889.53 1* / 
# 'BMB-05' OPEN GRAT 2* 6742.0 2* 282.76 1* / 
# 'BMB-06' OPEN GRAT 2* 9661.0 2* 269.72 1* / 
# 'BMB-07' OPEN GRAT 2* 4626.0 2* 259.44 1* / 
# 'BMD-01' OPEN GRAT 2* 10040.0 2* 281.14 1* / 
# 'BMD-02' STOP GRAT 2* 0.0 2* 297.37 1* / 
# 'BMD-03' OPEN GRAT 2* 6991.0 2* 258.99 1* / 
# 'BMD-04' OPEN GRAT 2* 10808.0 2* 281.77 1* / 
# 'BMD-05' STOP GRAT 2* 0.0 2* 256.55 1* / 
# 'BMD-06' STOP GRAT 2* 0.0 2* 306.5 1* / 
# 'BMD-07' STOP GRAT 2* 0.0 2* 306.35 1* / 
# 'BMD-08' STOP GRAT 2* 0.0 2* 163.12 1* / 
# / 
