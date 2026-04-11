# -*- coding: utf-8 -*-
"""
Created on Sun May 21 16:19:30 2023

@author: trent
"""
import boto3
# from pydfs_lineup_optimizer.solvers import PuLPSolver
# from random import shuffle, choice  
# from pulp import *
# from tqdm import tqdm
import numpy as np
import pandas as pd
# import random
import streamlit as st 
import sqlalchemy 
# import heapq
# import pickle
# from pathlib import Path
# import streamlit_authenticator as stauth
# import copy
# from pydfs_lineup_optimizer import get_optimizer,TeamStack,Site, Sport,Player,player,set_timezone,AfterEachExposureStrategy,RandomFantasyPointsStrategy,PlayersGroup,DataFrameLineupExporter
# import pandas as pd
# import numpy as np
# from pydfs_lineup_optimizer.solvers.mip_solver import MIPSolver
# from pydfs_lineup_optimizer.player import GameInfo
import requests
# import math
import datetime
import plotly.express as px
import psycopg2
# from stqdm import stqdm

@st.cache_data
def data_pull(file):
    project = obj.get_object(Bucket= 'cbbdata2023', Key= file) 
    frame = pd.read_csv(project['Body'])
    return frame

st.set_page_config(layout="wide")

obj = boto3.client("s3")

playerproj = data_pull('playerprojections.csv') 


playerproj = playerproj.drop_duplicates(subset='player')

cbb = data_pull('underdog.csv') 


cf_pivot = data_pull('prizepicks.csv') 



proppiv = data_pull('caesars.csv') 


nojuice = data_pull('nojuice.csv') 


projectionset=data_pull('projections.csv') 

df = data_pull('logs.csv')


gamebetting = data_pull('betting.csv')


player_stats_dict = playerproj.set_index(['player']).to_dict(orient = 'index')

proj_options = playerproj['player'].unique().tolist()
proj_team = playerproj['team'].unique().tolist()

stats= ['Points','Rebounds','Assists','PRA','Points + Assists','Points + Rebounds','3PM']


df=df[['athlete_display_name','game_date','team','opponent','athlete_position_abbreviation','minutes','pts','fgm','fga','threeptmade','threeptattempt','ftm','fta','reb','ast','stl','blk','to']]
df = df[df['athlete_display_name'].notnull()]
df= df.sort_values(by=['athlete_display_name'])
df['date'] =pd.to_datetime(df['game_date'])
df['date'] =df['date'].dt.date



player_options = df['athlete_display_name'].unique().tolist()
team_options = sorted(df['team'].unique().tolist())
dates=pd.DataFrame(pd.date_range('2023-11-01',datetime.date.today(),normalize=True))
dates=dates[0].dt.date
dates =dates.tolist()


# player = st.multiselect('Player',options = player_options)
# team = st.multiselect('Team',options =team_options)
# start_date,end_date  = st.slider('Daterange', min_value = dates[0] , max_value = dates[-1], value = (dates[0], dates[-1]) )


# newdates=pd.DataFrame(pd.date_range(start_date,end_date,normalize=True))
# newdates=newdates[0].dt.date
# newdates =newdates.tolist()

tab1,tab2,tab3,tab4,tab5 = st.tabs(["Game Logs",'Player Sims','Full Prop Sims','Player Projections','Game Betting Model'])

def convert_df_to_csv(df):
  # IMPORTANT: Cache the conversion to prevent computation on every rerun
  return df.to_csv(index =False).encode('utf-8')



with tab4:
    st.dataframe(projectionset)


# player_list = copy.deepcopy(player_dict)

# dk.columns

with tab1:
    hold_container = st.empty()
    # st.download_button(
    #     label="Export Projection Template",
    #     data=convert_df_to_csv(dk[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev', 'AvgPointsPerGame']]),
    #     file_name='Projectionupload.csv',
    #     mime='text/csv',
    # )
    st.info('Welcome to the Fast Break Fantasy CBB Analysis tool.')
    col1, col2 = st.columns([1, 6])
    with col1:
        player = st.multiselect('Player',options = player_options)
        team = st.multiselect('Team',options =team_options)
        start_date,end_date  = st.slider('Daterange', min_value = dates[0] , max_value = dates[-1], value = (dates[0], dates[-1]) )
    
    
        newdates=pd.DataFrame(pd.date_range(start_date,end_date,normalize=True))
        newdates=newdates[0].dt.date
        newdates =newdates.tolist()

    # uploaded_file = st.file_uploader("Choose a file")
    if len(player) >=1 & len(team) ==0:
        filtered_df= df[df['athlete_display_name'].isin(player)]
        filtered_df = filtered_df[filtered_df['date'].isin(newdates)]               
        st.dataframe(filtered_df) 
    elif len(player) ==0 and len(team) >0:
        filtered_df= df[df['team'].isin(team)]
        filtered_df = filtered_df[filtered_df['date'].isin(newdates)]        
        st.dataframe(filtered_df) 
    elif len(player) >0 and len(team) >0:
        c1 =df['team'].isin(team)
        c2 =df['athlete_display_name'].isin(player)
        filtered_df= df[c1 & c2]
        filtered_df = filtered_df[filtered_df['date'].isin(newdates)]       
        st.dataframe(filtered_df) 
    elif len(player) ==0 and len(team) ==0 :
        filtered_df =df
        filtered_df = filtered_df[filtered_df['date'].isin(newdates)]       
        st.dataframe(filtered_df)
        
    st.download_button(
                           label="Export Game Logs",
                           data=convert_df_to_csv(filtered_df),
                           file_name='cbb logs.csv',
                           mime='text/csv',
                       )

        
with tab2:
    st.info('CBB Player Sims')
    col1, col2 = st.columns([1, 5])
    with col2:
        df_hold_container = st.empty()
        info_hold_container = st.empty()
        plot_hold_container = st.empty()

    with col1:
        players = st.selectbox('Select Player',options = proj_options)
        # teams = st.multiselect('Select Team',options =proj_team)
        statname = st.selectbox('Select Stat Category',options = stats)
        # site = st.selectbox('Select a Site',options = ['Underdog','Prizepicks','Caesars'])
        
        if statname == 'Points':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 50.5, value = 15.5, step = .5)
        elif statname == '3PM':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 5.5, value = 1.5, step = .5)
        elif statname == 'Rebounds':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 25.5, value = 5.5, step = .5)
        elif statname == 'Assists':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 25.5, value = 5.5, step = .5)
        elif statname == 'PRA':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 65.5, value = 20.5, step = .5)
        elif statname == 'Points + Rebounds':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 45.5, value = 10.5, step = .5)
        elif statname == 'Points + Assists':
            prop_var = st.number_input('Type in the prop offered (i.e 5.5)', min_value = 0.0, max_value = 45.5, value = 10.5, step = .5)
            
        if st.button('Simulate'):
            playersim = player_stats_dict[players]
            pts_range = np.random.normal(playersim['Pts'],playersim['pts'],10000)
            reb_range = np.random.normal(playersim['Reb'],playersim['reb'],10000)
            ast_range = np.random.normal(playersim['Ast'],playersim['ast'],10000)
            blk_range = np.random.normal(playersim['Blk'],playersim['blk'],10000)
            stl_range = np.random.normal(playersim['Stl'],playersim['stl'],10000)
            threes_range = np.random.normal(playersim['3PM'],playersim['fg3m'],10000)
            try:
                if np.isnan(playersim['name']):
                    playersim['name'] = playersim['Player_x']
            except:
                pass
            simdf = pd.DataFrame({'Player':playersim['name'],'PtsSim':pts_range,'RebSim':reb_range,'AstSim':ast_range,'BlkSim':blk_range,'StlSim':stl_range,'ThreesSim':threes_range})
            
            if statname == 'Points':
                simdf['PointsOver'] = np.where(simdf['PtsSim'] >= prop_var,1,0) 
                outcomes = simdf['PtsSim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','PointsOver']].groupby('Player').sum() /10000
                over = overpercent['PointsOver'].iloc[0]
                x1 = simdf.PtsSim.to_numpy()
            elif statname == '3PM':
                simdf['ThreesOver'] = np.where(simdf['ThreesSim'] >= prop_var,1,0) 
                outcomes = simdf['ThreesSim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','ThreesOver']].groupby('Player').sum() /10000
                over = overpercent['ThreesOver'].iloc[0]
                x1 = simdf.ThreesSim.to_numpy()
            elif statname == 'Rebounds':
                simdf['RebOver'] = np.where(simdf['RebSim'] >= prop_var,1,0) 
                outcomes = simdf['RebSim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','RebOver']].groupby('Player').sum() /10000
                over = overpercent['RebOver'].iloc[0]
                x1 = simdf.RebSim.to_numpy()
            elif statname == 'Assists':
                simdf['AstOver'] = np.where(simdf['AstSim'] >= prop_var,1,0)  
                outcomes = simdf['AstSim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','AstOver']].groupby('Player').sum() /10000
                over = overpercent['AstOver'].iloc[0]
                x1 = simdf.AstSim.to_numpy()
            elif statname == 'PRA':
                simdf['PRAOver'] = np.where((simdf['PtsSim']+ simdf['RebSim'] + simdf['AstSim']) >= prop_var,1,0)  
                simdf['PRASim']= simdf['PtsSim']+ simdf['RebSim'] + simdf['AstSim']
                outcomes = simdf['PRASim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','PRAOver']].groupby('Player').sum() /10000
                over = overpercent['PRAOver'].iloc[0]
                x1 = simdf.PRASim.to_numpy()
            elif statname == 'Points + Rebounds':
                simdf['PROver'] = np.where((simdf['PtsSim']+ simdf['RebSim']) >= prop_var,1,0)  
                simdf['PRSim']= simdf['PtsSim']+ simdf['RebSim'] 
                outcomes = simdf['PRSim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','PROver']].groupby('Player').sum() /10000
                over = overpercent['PROver'].iloc[0]
                x1 = simdf.PRSim.to_numpy()
            elif statname == 'Points+ Assists':
                simdf['PAOver'] = np.where((simdf['PtsSim']+ simdf['AstSim']) >= prop_var,1,0)  
                simdf['PASim']= simdf['PtsSim']+simdf['AstSim']
                outcomes = simdf['PASim']
                final_out = outcomes.reset_index()
                final_out.columns =['Instance','Outcome']
                overpercent = simdf[['Player','PAOver']].groupby('Player').sum() /10000
                over = overpercent['PAOver'].iloc[0]
                x1 = simdf.PASim.to_numpy()
            # x1 = player_outcomes.Outcome.to_numpy()
    
            print(x1)
    
            hist_data = [x1]
    
            group_labels = ['player outcomes']
    
            fig = px.histogram(
                    final_out, x='Outcome')
            fig.add_vline(x=prop_var, line_dash="dash", line_color="green")
    
            # with df_hold_container:
            #     df_hold_container = st.empty()
            #     format_dict = {'10%': '{:.2f}', 'Mean_Outcome': '{:.2f}','90%': '{:.2f}', 'beat_prop': '{:.2%}','implied_odds': '{:.2%}'}
            #     st.dataframe(final_outcomes.style.format(format_dict), use_container_width = True)
    
            with info_hold_container:
                st.info('Simulation Results:')
                st.info(f'{players} is projected to go over {prop_var} {statname} {over:.0%} of the time')
            with plot_hold_container:
                st.dataframe(simdf, use_container_width = True)
                plot_hold_container = st.empty()
                st.plotly_chart(fig, use_container_width=True)
            
            
            

        
with tab3:   
    hold_container = st.empty()
    # st.info('This is the Fast Break WNBA optimizer. This optimizer will run up to 1000 lineups for the current main slate and simulate those created lineups against one another to determine the Top lineups to use. Projections are preloaded but you can add your own on the upload tab')
    # st.info(' You can now update the projections below. Change the value in the AvgPointPerGame column to adjust the players projection')
    col1, col2 = st.columns([2, 8])
    
    with col1:
      
        site = st.selectbox('Select a Site',options = ['Underdog','PrizePicks','Caesars'])
        if st.button('Simulate Site Props'):
            if site =='PrizePicks':
                ppresults = []
                for check in player_stats_dict:
                    try:
                        testplayer = player_stats_dict[check]
                        print(check)
                        pts_range = np.random.normal(testplayer['Pts'],testplayer['pts'],10000)
                        reb_range = np.random.normal(testplayer['Reb'],testplayer['reb'],10000)
                        ast_range = np.random.normal(testplayer['Ast'],testplayer['ast'],10000)
                        blk_range = np.random.normal(testplayer['Blk'],testplayer['blk'],10000)
                        stl_range = np.random.normal(testplayer['Stl'],testplayer['stl'],10000)
                        threes_range = np.random.normal(testplayer['3PM'],testplayer['fg3m'],10000)
                        df = pd.DataFrame({'Player':testplayer['attributes.name'],'PtsSim':pts_range,'RebSim':reb_range,'AstSim':ast_range,'BlkSim':blk_range,'StlSim':stl_range,'ThreesSim':threes_range})
                        ppdf = pd.merge(df,cf_pivot,how='inner',left_on='Player',right_on='attributes.name')   
                        ppdf['PointsOver'] = np.where(ppdf['PtsSim'] >= ppdf['Points'],1,0)     
                        ppdf['RebOver'] = np.where(ppdf['RebSim'] >= ppdf['Rebounds'],1,0)     
                        ppdf['AstOver'] = np.where(ppdf['AstSim'] >= ppdf['Assists'],1,0)     
                        try:
                            ppdf['BlkOver'] = np.where(ppdf['BlkSim'] >= ppdf['Blocked Shots'],1,0)
                        except:
                            ppdf['BlkOver'] = 0
                        try:
                            ppdf['StlOver'] = np.where(ppdf['StlSim'] >= ppdf['Steals'],1,0)    
                        except:
                            ppdf['StlOver']=0
                        ppdf['ThreesOver'] = np.where(ppdf['ThreesSim'] >= ppdf['3-PT Made'],1,0)     
                        try:
                            ppdf['BlkStlOver'] = np.where((ppdf['BlkSim'] + ppdf['StlSim']) >= ppdf['Blks+Stls'],1,0)     
                        except:
                            ppdf['BlkStlOver'] =0
                        try:
                            ppdf['PAOver'] = np.where((ppdf['PtsSim']+ ppdf['AstSim']) >= ppdf['Pts+Asts'],1,0)  
                        except:
                            ppdf['PAOver'] = 0
                        try:
                            ppdf['PROver'] = np.where((ppdf['PtsSim']+ ppdf['RebSim']) >= ppdf['Pts+Rebs'],1,0)  
                        except:
                            ppdf['PROver'] =0
                        try:
                            ppdf['PRAOver'] = np.where((ppdf['PtsSim']+ ppdf['RebSim'] + ppdf['AstSim']) >= ppdf['Pts+Rebs+Asts'],1,0)  
                        except:
                            ppdf['PRAOver'] = 0
                        try:            
                            ppdf['RAOver'] = np.where((ppdf['RebSim']+ ppdf['AstSim']) >= ppdf['Rebs+Asts'],1,0)  
                        except:
                            ppdf['RAOver'] =0
                        try:
                            ppdf['FantOver'] = np.where((ppdf['PtsSim'] + (1.25*ppdf['RebSim']) + (1.5*ppdf['AstSim']) + 3*(ppdf['StlSim'] + ppdf['BlkSim'])) > ppdf['Fantasy Score'],1,0)
                        except:
                            ppdf['FantOver'] = 0
                            
                        result = ppdf.groupby(['Player','attributes.odds_type'])[[ 'PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver', 'ThreesOver','BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver', 'FantOver']].sum().reset_index()
                        result = result[['Player','attributes.odds_type', 'PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver', 'ThreesOver','BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver', 'FantOver']]
                        result[['PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver',
                                'ThreesOver', 'BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver',
                                'FantOver']] = result[['PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver',
                                  'ThreesOver', 'BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver',
                                      'FantOver']].div(10000)
                        ppresults.append(result)
                    except ValueError:
                        pass
        
                ppovers = pd.concat(ppresults)
                ppovers = pd.merge(ppovers,cf_pivot,how='inner',left_on=['Player','attributes.odds_type'],right_on=['attributes.name','attributes.odds_type'])      
                ppovers['3-PT Made'] = ppovers.get('3-PT Made', float('NaN')) 
                ppovers['Blks+Stls'] = ppovers.get('Blks+Stls', float('NaN'))
                ppovers['Free Throws Made'] = ppovers.get('Free Throws Made', float('NaN'))
                ppovers['Turnovers'] = ppovers.get('Turnovers', float('NaN'))
                ppovers['Pts+Rebs'] = ppovers.get('Pts+Rebs', float('NaN'))
                ppovers['Pts+Asts'] = ppovers.get('Pts+Asts', float('NaN'))
                ppovers['Pts+Rebs+Asts'] = ppovers.get('Pts+Rebs+Asts', float('NaN'))
                ppovers['Fantasy Score'] = ppovers.get('Fantasy Score', float('NaN'))
                ppovers['Rebs+Asts'] = ppovers.get('Rebs+Asts', float('NaN'))   
                ppovers['Assists'] = ppovers.get('Assists', float('NaN'))  
                ppovers['Rebounds'] = ppovers.get('Rebounds', float('NaN'))  
                ppovers['Points'] = ppovers.get('Points', float('NaN'))  
          
                
               
                try:
                     
                    ppovers = ppovers[['Player','attributes.team', 'attributes.odds_type',  'Points', 'PointsOver','Rebounds', 'RebOver','Assists', 'AstOver','Blocked Shots', 'BlkOver','Steals', 'StlOver',
                        '3-PT Made','ThreesOver', 'Blks+Stls', 'BlkStlOver','Pts+Asts', 'PAOver', 'Pts+Rebs', 'PROver', 'Pts+Rebs+Asts','PRAOver','Rebs+Asts', 'RAOver',
                        'Fantasy Score','FantOver']]
                   
                except:
                    ppovers['Blks+Stls'] = 0
                    ppovers['Blocked Shots'] = 0
                    ppovers = ppovers[['Player','attributes.team','attributes.odds_type',  'Points', 'PointsOver','Rebounds', 'RebOver','Assists', 'AstOver','Blocked Shots', 'BlkOver','Steals', 'StlOver',
                        '3-PT Made','ThreesOver', 'Blks+Stls', 'BlkStlOver','Pts+Asts', 'PAOver', 'Pts+Rebs', 'PROver', 'Pts+Rebs+Asts','PRAOver','Rebs+Asts', 'RAOver',
                        'Fantasy Score','FantOver']]
                ppovers = ppovers.drop_duplicates(subset = ['Player','attributes.odds_type'])    
                with col2:
                    st.dataframe(ppovers)
            # ppovers.to_csv('CBB PP.csv')
            elif site =='Underdog':
                udresults = []
                for check in player_stats_dict:
                    try:
                        testplayer = player_stats_dict[check]
                        print(check)
                        pts_range = np.random.normal(testplayer['Pts'],testplayer['pts'],10000)
                        reb_range = np.random.normal(testplayer['Reb'],testplayer['reb'],10000)
                        ast_range = np.random.normal(testplayer['Ast'],testplayer['ast'],10000)
                        blk_range = np.random.normal(testplayer['Blk'],testplayer['blk'],10000)
                        stl_range = np.random.normal(testplayer['Stl'],testplayer['stl'],10000)
                        threes_range = np.random.normal(testplayer['3PM'],testplayer['fg3m'],10000)
                        df = pd.DataFrame({'Player':testplayer['name'],'PtsSim':pts_range,'RebSim':reb_range,'AstSim':ast_range,'BlkSim':blk_range,'StlSim':stl_range,'ThreesSim':threes_range})
                        ppdf = pd.merge(df,cbb,how='inner',left_on='Player',right_on='name')   
                        ppdf['PointsOver'] = np.where(ppdf['PtsSim'] >= ppdf['points'].astype(float),1,0)     
                        ppdf['RebOver'] = np.where(ppdf['RebSim'] >= ppdf['rebounds'].astype(float),1,0)     
                        ppdf['AstOver'] = np.where(ppdf['AstSim'] >= ppdf['assists'].astype(float),1,0)     
                        try:
                            ppdf['BlkOver'] = np.where(ppdf['BlkSim'] >= ppdf['blocked_shots'].astype(float),1,0)     
                        except:
                            ppdf['BlkOver'] =0
                            ppdf['blocked_shots'] = 0
                            
                        ppdf['StlOver'] = np.where(ppdf['StlSim'] >= ppdf['steals'].astype(float),1,0)        
                        ppdf['ThreesOver'] = np.where(ppdf['ThreesSim'] >= ppdf['three_points_made'].astype(float),1,0)     
                        try:
                            ppdf['BlkStlOver'] = np.where((ppdf['BlkSim'] + ppdf['StlSim']) >= ppdf['blks_stls'].astype(float),1,0)     
                        except:
                            ppdf['BlkStlOver'] =  0 
                        try:    
                            ppdf['PAOver'] = np.where((ppdf['PtsSim']+ ppdf['AstSim']) >= ppdf['pts_asts'].astype(float),1,0)  
                        except:
                            ppdf['PAOver'] = 0
                        ppdf['PROver'] = np.where((ppdf['PtsSim']+ ppdf['RebSim']) >= ppdf['pts_rebs'].astype(float),1,0)  
                        ppdf['PRAOver'] = np.where((ppdf['PtsSim']+ ppdf['RebSim'] + ppdf['AstSim']) >= ppdf['pts_rebs_asts'].astype(float),1,0)  
                        ppdf['RAOver'] = np.where((ppdf['RebSim']+ ppdf['AstSim']) >= ppdf['rebs_asts'].astype(float),1,0)  
                        try:
                            ppdf['FantOver'] = np.where((ppdf['PtsSim'] + (1.25*ppdf['RebSim']) + (1.5*ppdf['AstSim']) + 3*(ppdf['StlSim'] + ppdf['BlkSim'])) > ppdf['fantasy_points'].astype(float),1,0)
                        except:
                            ppdf['FantOver'] = 0
                            ppdf['fantasy_points'] = 0
                        result = ppdf.groupby('Player').sum().reset_index()
                        result = result[['Player', 'PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver', 'ThreesOver','BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver', 'FantOver']]
                        result[['PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver',
                                'ThreesOver', 'BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver',
                                'FantOver']] = result[['PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver',
                                      'ThreesOver', 'BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver',
                                      'FantOver']].div(10000)
                        udresults.append(result)
                    except ValueError:
                        pass
                    
                udovers = pd.concat(udresults)
                udovers = pd.merge(udovers,cbb,how='inner',left_on='Player',right_on='name')
                udovers['three_points_made'] = udovers.get('three_points_made', float('NaN')) 
                udovers['blks_stls'] = udovers.get('blks_stls', float('NaN'))
                udovers['blocked_shots'] = udovers.get('blocked_shots', float('NaN'))
                udovers['pts_rebs'] = udovers.get('pts_rebs', float('NaN'))
                udovers['pts_asts'] = udovers.get('pts_asts', float('NaN'))
                udovers['pts_rebs_asts'] = udovers.get('pts_rebs_asts', float('NaN'))
                udovers['fantasy_points'] = udovers.get('fantasy_points', float('NaN'))
                udovers['rebs_asts'] = udovers.get('rebs_asts', float('NaN'))   
                udovers['assists'] = udovers.get('assists', float('NaN'))  
                udovers['rebounds'] = udovers.get('rebounds', float('NaN'))  
                udovers['points'] = udovers.get('points', float('NaN'))  
                udovers['steals'] = udovers.get('steals', float('NaN'))  
                try:
                    udovers = udovers[['Player','points','PointsOver','rebounds', 'RebOver','assists', 'AstOver','blocked_shots', 'BlkOver','steals', 'StlOver',
                            'three_points_made','ThreesOver','blks_stls', 'BlkStlOver', 'pts_asts','PAOver', 'pts_rebs', 'PROver','pts_rebs_asts', 'PRAOver', 'rebs_asts', 'RAOver',
                            'fantasy_points','FantOver']]
                except:
                    udovers = udovers[['Player','points','PointsOver','rebounds', 'RebOver','assists', 'AstOver','blocked_shots', 'BlkOver','steals', 'StlOver',
                            'three_points_made','ThreesOver','blks_stls', 'BlkStlOver', 'pts_asts','PAOver', 'pts_rebs', 'PROver','pts_rebs_asts', 'PRAOver', 'rebs_asts', 'RAOver',
                            'fantasy_points','FantOver']]
                with col2:
                    st.dataframe(udovers)
            elif site =='Caesars':
                caesarsresults = []
                for check in player_stats_dict:
                    try:
                        testplayer = player_stats_dict[check]
                        print(check)
                        st.write(f"Simulating Stats for {check}")
                        pts_range = np.random.normal(testplayer['Pts'],testplayer['pts'],10000)
                        reb_range = np.random.normal(testplayer['Reb'],testplayer['reb'],10000)
                        ast_range = np.random.normal(testplayer['Ast'],testplayer['ast'],10000)
                        blk_range = np.random.normal(testplayer['Blk'],testplayer['blk'],10000)
                        stl_range = np.random.normal(testplayer['Stl'],testplayer['stl'],10000)
                        threes_range = np.random.normal(testplayer['3PM'],testplayer['fg3m'],10000)
                        df = pd.DataFrame({'Player':testplayer['name'],'PtsSim':pts_range,'RebSim':reb_range,'AstSim':ast_range,'BlkSim':blk_range,'StlSim':stl_range,'ThreesSim':threes_range})
                        ppdf = pd.merge(df,proppiv,how='inner',left_on='Player',right_on='Player')   
                        ppdf['PointsOver'] = np.where(ppdf['PtsSim'] >= ppdf['Player Total Points'].astype(float),1,0)     
                        ppdf['RebOver'] = np.where(ppdf['RebSim'] >= ppdf['Player Total Rebounds'].astype(float),1,0)     
                        ppdf['AstOver'] = np.where(ppdf['AstSim'] >= ppdf['Player Total Assists'].astype(float),1,0)     
                        try:
                            ppdf['BlkOver'] = np.where(ppdf['BlkSim'] >= ppdf['Player Total BLocks'].astype(float),1,0)     
                        except:
                            ppdf['BlkOver'] =0
                            ppdf['blocked_shots'] = 0
                        try:
                            ppdf['StlOver'] = np.where(ppdf['StlSim'] >= ppdf['Player Total Steals'].astype(float),1,0)        
                        except:
                            ppdf['StlOver'] = 0
                            
                        ppdf['ThreesOver'] = np.where(ppdf['ThreesSim'] >= ppdf['Player Total 3pt Field Goals'].astype(float),1,0)     
                        try:
                            ppdf['BlkStlOver'] = np.where((ppdf['BlkSim'] + ppdf['StlSim']) >= ppdf['Player Total Blocks + Steals'].astype(float),1,0)     
                        except:
                            ppdf['BlkStlOver'] = 0
                        try:    
                            ppdf['PAOver'] = np.where((ppdf['PtsSim']+ ppdf['AstSim']) >= ppdf['Player Total Points + Assists'].astype(float),1,0)  
                            ppdf['PROver'] = np.where((ppdf['PtsSim']+ ppdf['RebSim']) >= ppdf['Player Total Points + Rebounds'].astype(float),1,0)  
                            ppdf['PRAOver'] = np.where((ppdf['PtsSim']+ ppdf['RebSim'] + ppdf['AstSim']) >= ppdf['Player Total Points + Assists + Rebounds'].astype(float),1,0)  
                            ppdf['RAOver'] = np.where((ppdf['RebSim']+ ppdf['AstSim']) >= ppdf['Player Total Rebounds + Assists'].astype(float),1,0)  
                        except:
                            ppdf['PAOver'] = 0
                            ppdf['PROver'] = 0
                            ppdf['PRAOver'] = 0  
                            ppdf['RAOver'] = 0
                        ppdf['FantOver'] = 0
                        result = ppdf.groupby('Player').sum().reset_index()
                        result = result[['Player', 'PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver', 'ThreesOver','BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver', 'FantOver']]
                        result[['PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver',
                               'ThreesOver', 'BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver',
                               'FantOver']] = result[['PointsOver', 'RebOver', 'AstOver', 'BlkOver', 'StlOver',
                                      'ThreesOver', 'BlkStlOver', 'PAOver', 'PROver', 'PRAOver', 'RAOver',
                                      'FantOver']].div(10000)
                        caesarsresults.append(result)
                    except ValueError:
                        pass
                    
                caesarsovers = pd.concat(caesarsresults)
                caesarsovers = pd.merge(caesarsovers,proppiv,how='inner',left_on='Player',right_on='Player')
                caesarsovers =pd.merge(caesarsovers,nojuice,how='inner',left_on='Player',right_on='Player')
                caesarsovers = caesarsovers.drop_duplicates(subset = 'Player')
                caesarsovers = caesarsovers[['Player', 'Player Total Points_x','PointsOver','Player Total Rebounds_x', 'RebOver',
                                  'Player Total Assists_x','AstOver',
                       'Player Total 3pt Field Goals_x','ThreesOver']]
                with col2:
                    st.dataframe(caesarsovers)

with tab5:
    st.info("Welcome to the Fast Break Fantasy Hoops Game Betting Model. Here you can find our projected spread and projected total for each game")           
    st.dataframe(gamebetting)         
#     with col2:
#         # edit = st.experimental_data_editor(dk)
#         st.header('Optimizer Projections')
#         if uploaded_file is not None:
#             slate = pd.merge(proj_data,dk[['Name','Max Deviation','Projected Ownership','Max Exposure','Projection Floor']],how='left',on='Name')
#             slate['Projection Ceil'] = slate['AvgPointsPerGame']  + slate['Max Deviation']
#             edit = st.experimental_data_editor(slate[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info',
#                    'TeamAbbrev', 'AvgPointsPerGame','Projection Ceil', 'Projection Floor', 'Max Exposure',
#                    'Projected Ownership']])
#             # slate = slate[slate['AvgPointsPerGame'] > proj_cut]
#         else:
#             edit = st.experimental_data_editor(dk[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info',
#                    'TeamAbbrev', 'AvgPointsPerGame','Projection Ceil', 'Projection Floor', 'Max Exposure',
#                    'Projected Ownership']])
#             # slate = dk[dk['AvgPointsPerGame'] > proj_cut] 

        
#     if st.button('Optimize'):
#         max_proj = 1000
#         max_own = 1000
#         total_proj = 0
#         total_own = 0
        
#         with col2:
#             with st.spinner('Optimizing and Simulating Lineup Set'):
#                 with hold_container.container():
#                     slate =edit
#                     slate =edit[edit['AvgPointsPerGame'] > proj_cut] 
#                     slate['Max Exposure'] = np.where(slate['Max Exposure'] == max_exposure, max_exposure, slate['Max Exposure']) 
#                     games = dk['Game Info'].unique()
                    
#                     games[0].split('@')[1].split(' ')[2].strip()
#                     game_object =[]
#                     for i in range(len(games)):
#                         gameinfo = GameInfo(home_team = games[i].split('@')[1].split(' ')[0].strip() ,away_team = games[i].split('@')[0].strip() , starts_at =None)
#                         game_object.append(gameinfo)
                    
#                     game_key = []
                    
#                     for i in range(len(game_object)):
#                         away = game_object[i].away_team
#                         home = game_object[i].home_team
#                         away_dict = {away:game_object[i]}
#                         home_dict = {home:game_object[i]}
#                         game_key.append(away_dict)
#                         game_key.append(home_dict)
#                     players = [] 
                    
#                     for index, row in slate.iterrows():
#                         teamname = row['TeamAbbrev']
#                         search = [ele for ele in game_key if teamname in ele][0]
#                         game = search[teamname]
#                         positionlist = [row['Position'].split('/')[0],row['Position'].split('/')[1]]
#                         athlete = Player( 
#                         first_name = row['Name'].split(' ',1)[0]
#                         ,last_name = row['Name'].split(' ',1)[1]
#                         ,positions= positionlist
#                         ,player_id = row['ID']
#                         ,team = row['TeamAbbrev']
#                         ,salary = row['Salary']
#                         ,fppg = row['AvgPointsPerGame']
#                         ,fppg_ceil = row['Projection Ceil']
#                         ,fppg_floor = row['Projection Floor']
#                         ,max_exposure =np.where(row['Max Exposure'] == max_exposure, max_exposure, row['Max Exposure']/100)
#                         ,game_info  =game
#                         )
#                         players.append(athlete)
            
#                     lineups = []
#                     num =0
#                     optimizer = get_optimizer(Site.DRAFTKINGS,Sport.WNBA)
#                     optimizer.settings.budget = max_sal
#                     optimizer.player_pool.load_players(players)
#                     optimizer.set_min_salary_cap(min_sal)
#                     if slack_var >0:
#                         optimizer.set_fantasy_points_strategy(RandomFantasyPointsStrategy())
                    
#                     if lock_players is not None:
#                         for i in range(len(lock_players)):
#                             optimizer.player_pool.lock_player(lock_players[i])
                    
#                     if exclude_players is not None:
#                         for i in range(len(exclude_players)):
#                             optimizer.player_pool.remove_player(exclude_players[i])
                    
#                     if st.session_state['groups'] is not None:
#                         for i in range(len(st.session_state['groups'])):
#                             tobegrouped = []
#                             grouping = st.session_state['groups'][i]
#                             playergrouping = grouping[0]
#                             maxgroup = grouping[1]
#                             mingroup = grouping[2]
#                             for guy in optimizer.players:
#                                 if guy.full_name in playergrouping:
#                                     tobegrouped.append(guy)
#                             optimizer.add_players_group(PlayersGroup(tobegrouped,max_from_group=maxgroup, min_from_group = mingroup))
                            
                            
#                     for lineup in optimizer.optimize(n=totalRuns_raw,exposure_strategy= AfterEachExposureStrategy):
#                         num+=1
#                         print(num)
#                         lineups.append(lineup)
                    
                                        
#                     main = {}
                    
#                     for i in range(len(lineups)):
#                         line = lineups[i]
#                         for player in line:
#                             if player.id in main:
#                                 main[player.id] += 1
#                             else:
#                                 main[player.id] = 1
                    
#                     all_players = pd.DataFrame(main.items()) 
#                     all_players.columns = ['ID','# of Lineups']
#                     all_players['ID'] = all_players['ID'].astype(int)
#                     all_players['exposure %'] = all_players['# of Lineups'] / len(lineups)
#                     all_players = pd.merge(all_players,slate[['ID','Name']],how='left', left_on ='ID',right_on='ID')
                            
#                     players = set()
#                     for r in lineups:
#                         for key in main:
#                             players.add(key)
                    
#                     sorted_names = sorted(players)
#                     player_matrix = np.zeros((len(players), len(players)), dtype=int)
                    
#                     for r in range(len(lineups)):
#                         linecheck = lineups[r]
#                         team =[]
#                         for player in linecheck:
#                             team.append(player.id)
#                         for i, p1 in enumerate(sorted_names):
#                             for j, p2 in enumerate(sorted_names):
#                                 if p1 in team and p2 in team:
#                                     player_matrix[i, j] += 1
                    
#                     rows = [[''] + sorted_names]
                    
#                     for i, p in enumerate(sorted_names):
#                         rows.append([p] + list(player_matrix[i, :]))
                    
#                     corre = pd.DataFrame(rows) 
                    
#                     new = corre.iloc[0]
#                     new[0] = 'Player'
#                     corre = corre[1:]
#                     corre.columns = new
                    
                    
#                     cols_to_sum = corre.columns[1 : corre.shape[1]]
#                     corre[cols_to_sum] = corre[cols_to_sum].apply(pd.to_numeric)
#                     corre['Total'] = corre[cols_to_sum].max(axis=1)
                    
#                     raw_corre = corre.set_index('Player')
                    
                    
#                     cols_to_div = corre.columns[1:corre.shape[1]]
                    
#                     corre[cols_to_div] = corre[cols_to_div].divide(corre['Total'].values,axis=0)
                    
#                     main = corre.set_index('Player')      
                    
#                     playerlist =[]
                    
#                     for i in range(len(corre.Player)):
#                         first_player = corre.iloc[i,0]
#                         for i in range(1,(corre.shape[1]-2)):
#                             second_player = corre.columns[i]
#                             pair = [first_player,second_player]
#                             playerlist.append(pair)
                            
#                     playersets = pd.DataFrame(playerlist, columns = ['Player 1', ' Player 2'])
                    
                    
                    
#                     corr_table =[]
#                     for i in range(len(playersets['Player 1'])):
#                         first_player = playersets.iloc[i,0]
#                         second_player = playersets.iloc[i,1]
#                         correl = main.loc[first_player].at[second_player]
#                         totalcorr = raw_corre.loc[first_player].at[second_player]
#                         port = totalcorr/len(lineups)
#                         pair = [first_player,second_player, correl , totalcorr,port]
#                         corr_table.append(pair)
                        
#                     portfolio = pd.DataFrame(corr_table, columns = ['Player A', 'Player B','R Value','Total Lineups','% of Portfolio'])
                    
#                     portfolio = portfolio[portfolio['Total Lineups'] > 0]
#                     portfolio['Check'] = portfolio['Player A'] == portfolio['Player B']
#                     portfolio = portfolio[portfolio['Check'] == False]
                    
#                     portfolio = portfolio[['Player A', 'Player B', 'R Value', 'Total Lineups', '% of Portfolio']]
                    
#                     diverse = portfolio.groupby('Player A').agg({'% of Portfolio':'max', 'R Value':'mean'})
                    
#                     diverse['Variance Factor'] = diverse['% of Portfolio'] * diverse['R Value']
#                     diverse = diverse.reset_index()
                    
#                     diverse['Player A'] = diverse['Player A'].astype(int)
#                     diverse = pd.merge(diverse,slate[['ID','Name']],how='left', left_on ='Player A',right_on='ID')
#                     portfolio['Player A'] = portfolio['Player A'].astype(int)
#                     portfolio['Player B'] = portfolio['Player B'].astype(int)
                    
#                     portfolio = pd.merge(portfolio,slate[['ID','Name']],how='left', left_on ='Player A',right_on='ID')
#                     portfolio = pd.merge(portfolio,slate[['ID','Name']],how='left', left_on ='Player B',right_on='ID')
                    
#                     diverse = diverse[['Name','Player A', '% of Portfolio', 'R Value', 'Variance Factor']]
                    
#                     portfolio = portfolio[['Name_x','Player A',  'Name_y','Player B', 'R Value', 'Total Lineups', '% of Portfolio']]
                        
#                     lineupset =DataFrameLineupExporter(lineups)

#                     lines =lineupset.export()
#                     lines.columns = ['G', 'G.1', 'F', 'F.1', 'F.2', 'UTIL','Salary','Fpts Proj']
#                     linestart = lines.reset_index()
#                     linestart['Lineup'] = "lineup" + linestart['index'].astype(str)

#                     lines = lines[['G', 'G.1', 'F', 'F.1', 'F.2', 'UTIL']]
#                     player_dict = {}
                    
#                     for i in range(len(slate)):
#                         check = slate.iloc[i,:]
#                         player_name = check['Name']
#                         player_dict[player_name] = {'Fpts': 0, 'Position': [], 'ID': 0, 'Salary': 0, 'StdDev': 0, 'Ceiling': 0, 'Ownership': 0.1, 'In Lineup': False,'Team':0,'Max Exposure':1,'Count':0,'Build %':0}
#                         player_dict[player_name]['Fpts'] = float(check['AvgPointsPerGame'])
#                         player_dict[player_name]['Salary'] = int(check['Salary'])
#                         player_dict[player_name]['Position'] = str(check['Position'])
#                         # player_dict[player_name]['Position'] = [pos for pos in check['Roster Position'].split('/')][0]
#                         player_dict[player_name]['Ownership'] = float(check['Projected Ownership'])
#                         player_dict[player_name]['StdDev'] = float(check['Projection Ceil'] - check['AvgPointsPerGame'] )
#                         player_dict[player_name]['Ceiling'] = float(check['Projection Ceil'])
#                         player_dict[player_name]['ID'] = int(check['ID'])
#                         player_dict[player_name]['Team'] = str(check['TeamAbbrev'])
#                         player_dict[player_name]['Max Exposure'] = float(check['Max Exposure'])
#                         player_dict[player_name]['Count'] = 0  


#                     player_list = copy.deepcopy(player_dict)
                    
#                     check_lines =[]
#                     for i in range(len(lines)):
#                         line = lines.iloc[i,:]
#                         frame = pd.DataFrame(line)
#                         frame.columns = ['Player']
#                         line_to_sim = pd.merge(frame,dk[['Key','Name','AvgPointsPerGame','Max Deviation','Projected Ownership']],how='left',left_on='Player',right_on ='Key')
#                         line_to_sim['STD'] =line_to_sim['Max Deviation']
#                         check_lines.append(line_to_sim)
                    
#                     def simulate(mean,std):
#                           sim = mean +((random.randint(-100,100)/100) * std)
#                           return sim
                    
#                     field_lineups = {}

#                     for i in range(len(check_lines)):
#                         lineup = check_lines[i]['Name'].tolist()
#                         field_lineups[i] = {'Lineup': lineup, 'Wins': 0, 'Top10': 0, 'ROI': 0}
                        
#                     sims=10000
#                     for i in tqdm(range(0,(sims)),desc="Running Sims...",ascii=False,ncols=75): 
#                         temp_fpts_dict = {p: round((np.random.normal(stats['Fpts'], stats['StdDev'])), 2) for p,stats in player_dict.items()}
#                         field_score = {}
                        
#                         for index,values in field_lineups.items():
#                                 fpts_sim = sum(temp_fpts_dict[player] for player in values['Lineup'])
#                                 field_score[fpts_sim] = {'Lineup': values['Lineup'], 'Fpts': fpts_sim, 'Index': index}
#                         top_10 = heapq.nlargest(np.where(math.ceil(len(check_lines)*.10) <1,1,math.ceil(len(check_lines)*.10)), field_score.values(), key=lambda x: x['Fpts'])
#                         for lineup in top_10:
#                             if lineup == top_10[0]:
#                                 field_lineups[lineup['Index']]['Wins'] += 1

#                             field_lineups[lineup['Index']]['Top10'] += 1 
#                     print(str(sims) + ' tournament simulations finished. Outputting.')
                    
                    
#                     unique = {}
#                     for index, x in field_lineups.items():
#                         salary = sum(player_dict[player]['Salary'] for player in x['Lineup'])
#                         fpts_p = sum(player_list[player]['Fpts'] for player in x['Lineup'])
#                         ceil_p = sum(player_list[player]['Ceiling'] for player in x['Lineup'])
#                         own_p = sum([player_dict[player]['Ownership'] for player in x['Lineup']])
#                         win_p = round(x['Wins']/sims * 100, 2)
#                         own_p = float(own_p)
#                         top10_p = round(x['Top10']/sims * 100, 2)
#                         lineup_str = [x['Lineup'][0],x['Lineup'][1],x['Lineup'][2],x['Lineup'][3],x['Lineup'][4],x['Lineup'][5],salary,fpts_p,ceil_p,own_p,win_p,top10_p ]
#                         unique[index] = lineup_str

                    
#                     lineupset =[]
#                     for fpts, lineup_str in unique.items():
#                         lineupset.append(lineup_str)
                    
#                     simsresult = pd.DataFrame(lineupset, columns = ['G','G2','F','F2','F3','Util','Salary','Projection','Ceiling','Own Sum','Win%','Top10%'])
#                     simsresult = simsresult.sort_values(by ='Win%', ascending=False)             
#                     name_replace = dict(zip(slate['Name'], slate['Name + ID']))
#                     dkload = simsresult.replace(name_replace)
                      
#                 st.header('Generated Lineups')
#                 st.dataframe(dkload)
#                 with col3:
#                     st.header('Player Exposures')
#                     all_players = all_players[['Name','# of Lineups','exposure %']]
#                     all_players['exposure %'] = all_players['exposure %'] *100
#                     st.dataframe(all_players.set_index(all_players.columns[0]))
#                     # st.markdown(all_players.style.hide(axis="index").to_html(), unsafe_allow_html=True)
#                     # st.dataframe(diverse)
#                     st.header('Player Lineup Correlations')
#                     st.dataframe(portfolio.set_index(portfolio.columns[0]))
                
#                 with col1:
#                     st.download_button(
#                         label="Export Lineups",
#                         data=convert_df_to_csv(dkload),
#                         file_name='WNBA_DFS_export.csv',
#                         mime='text/csv',
#                     )

#                 with hold_container:
#                     hold_container = st.empty()
# with tab4:
#     col1 , col2 = st.columns([4,4])
#     with col1:
#         groupplayers = st.multiselect('Player Groups',player_options)
#         max_play = st.number_input('Max Per Group', min_value = 1, max_value = 6,value = 1, step = 1)
#         min_play = st.number_input('Min Per Group', min_value = 0, max_value = 6, value = 0, step = 1)
#         st.write(st.session_state['groups'])
#         if st.button('Group'):
#             groupdata([groupplayers,max_play,min_play])
#             st.write(st.session_state['groups'])
#     with col2:
#         group_number = st.selectbox('Group',list(st.session_state['groups']))
#         if st.button('Remove'):
#             st.session_state['groups'].remove(group_number)
#             st.write(st.session_state['groups'])
#        #     group = PlayersGroup(
#        #                         optimizer.player_pool.get_players(groupplayers[0],groupplayers[1]),
#        #                         max_from_group=max_play,
#        #                     )
#         if st.button('Clear All'):
#             st.session_state['groups'] = []
#             st.write(st.session_state['groups'])
              
# with tab5:

#     hold_container = st.empty()
#     col1, col2 = st.columns([1, 4])
#     with col1:
#         st.download_button(
#             label="Export Lineup Template",
#             data=convert_df_to_csv(pd.DataFrame(columns = ['G', 'G.1', 'F', 'F.1', 'F.2', 'UTIL'])),
#             file_name='Lineupupload.csv',
#             mime='text/csv',
#         )
#     st.info('Simulator has your lineups compete against each other and displays the probability that each lineup comes in 1st and the Top 10  ')
#     st.info("Upload your lineup portfolio to pick the best one or load up your lineups along with a set of projected contest lineups to see how they do  \n The upload has to be a CSV that contains just your lineups and they have to be in the Draftkings Name + ID format or just the player ID . ie. Nneka Ogwumike (28473490)  \n Download template above")
#     upload_file = st.file_uploader("Upload your lineups")
#     if upload_file is not None:
#       loadedlineups = pd.read_csv(upload_file)
#       loadedlineups = loadedlineups.replace(',','', regex=True)
#       loadedlineups = loadedlineups.iloc[:,[0,1,2,3,4,5]]
#       player_replace = {'Azura Stevens (28636397)':'AzurÃ¡ Stevens (28636397)'}
#       loadedlineups = loadedlineups.replace(player_replace)
#       if isinstance(loadedlineups.loc[0][0], np.integer) == True:
#           name_replacesims = dict(zip(dk['ID'], dk['Name + ID']))
#           loadedlineups = loadedlineups.replace(name_replacesims)
#       else: 
#           pass
#       st.write(loadedlineups)
#       if st.button('Simulate'):
          
#         simplayer_dict = {}
        
#         if uploaded_file is not None:
                
#             for i in range(len(slate1)):
#                 check = slate1.iloc[i,:]
#                 player_name = check['Name']
#                 simplayer_dict[player_name] = {'Fpts': 0, 'Position': [], 'ID': 0, 'Salary': 0, 'StdDev': 0, 'Ceiling': 0, 'Ownership': 0.1, 'In Lineup': False,'Team':0,'Max Exposure':1,'Count':0,'Build %':0}
#                 simplayer_dict[player_name]['Fpts'] = float(check['AvgPointsPerGame'])
#                 simplayer_dict[player_name]['Salary'] = int(check['Salary'])
#                 simplayer_dict[player_name]['Position'] = str(check['Position'])
#                 # player_dict[player_name]['Position'] = [pos for pos in check['Roster Position'].split('/')][0]
#                 simplayer_dict[player_name]['Ownership'] = float(check['Projected Ownership'])
#                 simplayer_dict[player_name]['StdDev'] = float(np.where(check['Projection Ceil'] > check['AvgPointsPerGame'] , 0 , check['Projection Ceil'] - check['AvgPointsPerGame']))
#                 simplayer_dict[player_name]['Ceiling'] = float(check['Projection Ceil'])
#                 simplayer_dict[player_name]['ID'] = int(check['ID'])
#                 simplayer_dict[player_name]['Team'] = str(check['TeamAbbrev'])
#                 simplayer_dict[player_name]['Max Exposure'] = float(check['Max Exposure'])
#                 simplayer_dict[player_name]['Count'] = 0  
#         else:
#             for i in range(len(dk)):
#                 check = dk.iloc[i,:]
#                 player_name = check['Name']
#                 simplayer_dict[player_name] = {'Fpts': 0, 'Position': [], 'ID': 0, 'Salary': 0, 'StdDev': 0, 'Ceiling': 0, 'Ownership': 0.1, 'In Lineup': False,'Team':0,'Max Exposure':1,'Count':0,'Build %':0}
#                 simplayer_dict[player_name]['Fpts'] = float(check['AvgPointsPerGame'])
#                 simplayer_dict[player_name]['Salary'] = int(check['Salary'])
#                 simplayer_dict[player_name]['Position'] = str(check['Position'])
#                 # player_dict[player_name]['Position'] = [pos for pos in check['Roster Position'].split('/')][0]
#                 simplayer_dict[player_name]['Ownership'] = float(check['Projected Ownership'])
#                 simplayer_dict[player_name]['StdDev'] = float(np.where(check['Projection Ceil'] > check['AvgPointsPerGame'] , 0 , check['Projection Ceil'] - check['AvgPointsPerGame']))
#                 simplayer_dict[player_name]['Ceiling'] = float(check['Projection Ceil'])
#                 simplayer_dict[player_name]['ID'] = int(check['ID'])
#                 simplayer_dict[player_name]['Team'] = str(check['TeamAbbrev'])
#                 simplayer_dict[player_name]['Max Exposure'] = float(check['Max Exposure'])
#                 simplayer_dict[player_name]['Count'] = 0  
            

#         # name_replaceone = dict(zip(dk['Name + ID'], dk['Name']))
        
#         # loadedlineups = loadedlineups.replace(name_replaceone)
        
#         # st.write(loadedlineups)
#         check_lines =[]
#         for i in range(len(loadedlineups)):
#             line = loadedlineups.iloc[i,:]
#             frame = pd.DataFrame(line)
#             frame.columns = ['Player']
#             line_to_sim = pd.merge(frame,dk[['Name + ID','Name','AvgPointsPerGame','Max Deviation','Projected Ownership']],how='left',left_on='Player',right_on ='Name + ID')
#             line_to_sim['STD'] =line_to_sim['Max Deviation']
#             check_lines.append(line_to_sim)
        
#         def simulate(mean,std):
#               sim = mean +((random.randint(-100,100)/100) * std)
#               return sim
        
#         field_lineups = {}

#         for i in range(len(check_lines)):
#             lineup = check_lines[i]['Name'].tolist()
#             field_lineups[i] = {'Lineup': lineup, 'Wins': 0, 'Top10': 0, 'ROI': 0}
            
#         sims=10000
#         for i in tqdm(range(0,(sims)),desc="Running Sims...",ascii=False,ncols=75): 
#             temp_fpts_dict = {p: round((np.random.normal(stats['Fpts'], stats['StdDev'])), 2) for p,stats in simplayer_dict.items()}
#             field_score = {}
            
#             for index,values in field_lineups.items():
#                     fpts_sim = sum(temp_fpts_dict[player] for player in values['Lineup'])
#                     field_score[fpts_sim] = {'Lineup': values['Lineup'], 'Fpts': fpts_sim, 'Index': index}
#             top_10 = heapq.nlargest(np.where(math.ceil(len(check_lines)*.10) <1,1,math.ceil(len(check_lines)*.10)), field_score.values(), key=lambda x: x['Fpts'])
#             for lineup in top_10:
#                 if lineup == top_10[0]:
#                     field_lineups[lineup['Index']]['Wins'] += 1

#                 field_lineups[lineup['Index']]['Top10'] += 1 
#         print(str(sims) + ' tournament simulations finished. Outputting.')
        
        
#         unique = {}
#         for index, x in field_lineups.items():
#             salary = sum(simplayer_dict[player]['Salary'] for player in x['Lineup'])
#             fpts_p = sum(simplayer_dict[player]['Fpts'] for player in x['Lineup'])
#             ceil_p = sum(simplayer_dict[player]['Ceiling'] for player in x['Lineup'])
#             own_p = sum([simplayer_dict[player]['Ownership'] for player in x['Lineup']])
#             win_p = round(x['Wins']/sims * 100, 2)
#             own_p = float(own_p)
#             top10_p = round(x['Top10']/sims * 100, 2)
#             lineup_str = [x['Lineup'][0],x['Lineup'][1],x['Lineup'][2],x['Lineup'][3],x['Lineup'][4],x['Lineup'][5],salary,fpts_p,ceil_p,own_p,win_p,top10_p ]
#             unique[index] = lineup_str

        
#         lineupset =[]
#         for fpts, lineup_str in unique.items():
#             lineupset.append(lineup_str)
        
#         simsresult = pd.DataFrame(lineupset, columns = ['G','G2','F','F2','F3','Util','Salary','Projection','Ceiling','Own Sum','Win%','Top10%'])
#         simsresult = simsresult.sort_values(by ='Win%', ascending=False)             
#         name_replace = dict(zip(dk['Name'], dk['Name + ID']))
#         simulationsresults = simsresult.replace(name_replace)
#         st.dataframe(simulationsresults)
        
              
#         st.download_button(
#             label="Export Lineup Simulation",
#             data=convert_df_to_csv(simulationsresults),
#             file_name='WNBA_DFS_Sims_export.csv',
#             mime='text/csv',
#         )