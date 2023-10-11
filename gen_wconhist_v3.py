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
conn = pyodbc.connect(driver='***', server='***', user='***', password='***', database='****')
cursor = conn.cursor()
bm = pd.read_sql_query(query, conn)



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
        
        

 

