# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 15:09:57 2025

@author: trent
"""
import gspread
from supabase_db_conn import get_db_engine
import pandas as pd
from dateutil.parser import parse
import datetime
from pytz import timezone
from predict_own import predict_ownership as po
import numpy as np
from pathlib import Path
import os, traceback


p = Path(r"C:\\Users\\Trent\\WNBA\\")
print("Path:", p)
print("Exists:", p.exists(), "| Is file:", p.is_file(), "| Is dir:", p.is_dir())
print("Parent writable:", os.access(p.parent, os.W_OK))



def load_to_google():
        engine = get_db_engine()


        credentials_path = os.environ.get(
            "GOOGLE_SHEETS_CREDENTIALS_FILE",
            "wnba-files-c2213e18569e.json",
        )
        gc = gspread.service_account(filename=credentials_path)

        median_sql = """select * from wnba.medians"""



        pp5_sql = """select * from wnba.pp5over"""


        pp10_sql = """select * from wnba.pp10over"""

        ud5_sql = """select * from wnba.ud5over"""


        ud10_sql = """select * from wnba.ud10over"""


        pp_sql = """select * from wnba.ppovers"""


        ud_sql = """select * from wnba.udovers"""
        
        std_sql = """select * from wnba.standard"""

        


        median_df = pd.read_sql(median_sql, engine).fillna(0)


        pp5_df = pd.read_sql(pp5_sql, engine).fillna(0)

        pp10_df = pd.read_sql(pp10_sql, engine).fillna(0)

        ud5_df = pd.read_sql(ud5_sql, engine).fillna(0)

        ud10_df = pd.read_sql(ud10_sql, engine).fillna(0)

        pp_df = pd.read_sql(pp_sql, engine).fillna(0)

        ud_df = pd.read_sql(ud_sql, engine).fillna(0)
        
        std_df = pd.read_sql(std_sql,engine).fillna(0)

        dk = pd.read_csv('wnbadk.csv')
        
        slate = pd.merge(median_df,dk, how='left', right_on ='Name',left_on='player_name')
        
        slate['value'] = (slate['dkpts']/slate['Salary']) *1000
        
        slate.to_csv(str(p) + '\\wnbaslate.csv')
        
        dklist = slate[['Position', 'Name + ID', 'Name', 'ID', 'Salary','Game Info', 'TeamAbbrev_y', 'AvgPointsPerGame','dkpts']]
        dkmain = pd.merge(dklist,std_df, how='left', left_on='Name', right_on ='player_name')
        dkmain['AvgPointsPerGame'] = dkmain.dkpts.fillna(0)
        dkmain['dkstd'] = dkmain.dkstd.astype(float)
        dkmain['Projection Ceil'] = dkmain['AvgPointsPerGame'] + dkmain.dkstd
        dkmain['Projection Floor'] =np.where(dkmain['AvgPointsPerGame'] - dkmain.dkstd <0 ,0,dkmain['AvgPointsPerGame'] - dkmain.dkstd)
        dkmain['Max Exposure'] = .80
        
        dkmain['Name'] = dkmain['Name'].str.replace('-',' ')
        dkmain['Name + ID'] = dkmain['Name + ID'].str.replace('-',' ')
  

        rawdata =  dkmain
        rawdata['Projected Ownership'] =0
        
        rawdata['Max Deviation'] = rawdata['Projection Ceil'] - rawdata['AvgPointsPerGame']
        rawdata['Key'] = rawdata['Name'] +'('+rawdata['ID'].astype(str) +')'
        rawdata['GPP'] = ((rawdata['Salary'] /1000) *3) +10
        rawdata['Play'] = rawdata['Projection Ceil'] - rawdata['GPP'] 
        
        
        
        rawdata[['Away','@','Home','Time']] = rawdata['Game Info'].str.split(' ',n=4,expand = True)
        
        rawdata['Time']  = [parse(x) for x in rawdata['Time']]
        
        rawdata['Time'] = [x.replace(tzinfo=datetime.timezone.utc) for x in rawdata['Time']]
              
        rawdata['Time'] =  [x.astimezone(timezone('US/Eastern')) for x in rawdata['Time']]      
        
        rawdata['Time'] = [x.strftime("%m/%d/%Y %I:%M%p %Z") for x in rawdata['Time']]
        
        rawdata['Game Info'] = rawdata['Away'] + '@' +rawdata['Home'] +' ' + rawdata['Time']
        
        rawdata['Max Exposure'] = 100
        
        rawdata =rawdata[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info',
               'TeamAbbrev_y', 'AvgPointsPerGame', 'dkpts', 'player_name', 'dkstd',
               'Projection Ceil', 'Projection Floor', 'Max Exposure',
               'Projected Ownership', 'Max Deviation', 'Key', 'GPP', 'Play']]
        
        rawdata =rawdata.fillna(0)
        rawdata.to_csv(str(p)+ '\\wnbaload.csv',index =False)
        
        
        
        medianload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1q6JpRRdHLZB2-w8IO7BPIIDdVWWYBXPqYEVonZGEfuk/edit?gid=1465431414').get_worksheet(0)

        medianload.batch_clear(['A2:Z500'])
        medianload.update('A2', median_df.values.tolist())


        pp5load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yCoJ88uBREbuPKe_s5pTg3m1MKznMLw4YjlKSHlkjho/edit?gid=191792048#gid=191792048').get_worksheet(0)
        pp10load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1yCoJ88uBREbuPKe_s5pTg3m1MKznMLw4YjlKSHlkjho/edit?gid=191792048#gid=191792048').get_worksheet(1)

        try:
                pp5load.batch_clear(['A1:Z500'])
                pp5load.update('A2', pp5_df.values.tolist())
                pp5load.update('A1', [pp5_df.columns.values.tolist()])
                
                pp10load.batch_clear(['A1:Z1000'])
                pp10load.update('A2', pp10_df.values.tolist())
                pp10load.update('A1', [pp10_df.columns.values.tolist()])
        except:
                pass
        ud5load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xI4gyjBn0XWO6v3q23FFzISs_aU047IRFUngYzlCSD0/edit?gid=0#gid=0').get_worksheet(0)
        ud10load = gc.open_by_url('https://docs.google.com/spreadsheets/d/1xI4gyjBn0XWO6v3q23FFzISs_aU047IRFUngYzlCSD0/edit?gid=0#gid=0').get_worksheet(1)

        try:
                ud5load.batch_clear(['A1:Z1000'])
                ud5load.update('A2', ud5_df.values.tolist())
                ud5load.update('A1', [ud5_df.columns.values.tolist()])
        except:
                pass

        try:
                ud10load.batch_clear(['A1:Z1000'])
                ud10load.update('A2', ud10_df.values.tolist())
                ud10load.update('A1', [ud10_df.columns.values.tolist()])
        except:
                pass


        udpremload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1iIMDBcp9u0sfz6vjgrY6eSaoX3Dh_Cx7H-cMj0wpPIQ/edit?gid=0#gid=0').get_worksheet(0)
        pppremload = gc.open_by_url('https://docs.google.com/spreadsheets/d/1e0ryQXY_WDw72QbOVm7bWXDZX9JKbxvtbnjRVzrewqE/edit?gid=0#gid=0').get_worksheet(0)

        try:
                udpremload.batch_clear(['A1:Z1000'])
                udpremload.update('A2', ud_df.values.tolist())
                udpremload.update('A1', [ud_df.columns.values.tolist()])
        except:
                pass

        try:
                pppremload.batch_clear(['A1:Z1000'])
                pppremload.update('A2', pp_df.values.tolist())
                pppremload.update('A1', [pp_df.columns.values.tolist()])
        except:
                pass

        

        wnba = gc.open_by_url('https://docs.google.com/spreadsheets/d/1r78ZTtFK99J4HBgK-YeJncR7ruyTMycUVG6f7VogTCU/edit').sheet1
        
        projupload = po()
        
        wnba.batch_clear(['A2:Z550'])
        wnba.update('A2', projupload.values.tolist())

        
