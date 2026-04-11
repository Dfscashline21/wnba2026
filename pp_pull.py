# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 16:05:54 2025

@author: trent
"""
import requests 
import pandas as pd 


    
def call_endpoint(url, max_level=3, include_new_player_attributes=False):
        '''
        takes: 
            - url (str): the API endpoint to call
            - max_level (int): level of json normalizing to apply
            - include_player_attributes (bool): whether to include player object attributes in the returned dataframe
        returns:
            - df (pd.DataFrame): a dataframe of the call response content
        '''
        resp = requests.get(url).json()
        data = pd.json_normalize(resp['data'], max_level=max_level)
        included = pd.json_normalize(resp['included'], max_level=max_level)
        if include_new_player_attributes:
            inc_cop = included[included['type'] == 'new_player'].copy().dropna(axis=1)
            data = pd.merge(data, inc_cop, how='left', left_on=['relationships.new_player.data.id','relationships.new_player.data.type'], right_on=['id','type'], suffixes=('', '_new_player'))
        return data


def prizepicks_pull():
    try:
            proj_prize  = 'https://partner-api.prizepicks.com/projections?league_id=3&per_page=1000&single_stat=true'
            fb_projection = call_endpoint(proj_prize, include_new_player_attributes=True)
            fb_projection2 = fb_projection[fb_projection['attributes.odds_type'] == 'demon']
            # fb_projection = fb_projection[fb_projection['attributes.odds_type'] == 'standard']
            fb_projection = fb_projection[['attributes.name','attributes.team','attributes.position','attributes.stat_type','attributes.line_score','attributes.odds_type']]
            fb_projection['attributes.line_score'] = fb_projection['attributes.line_score'].astype(float)   
            fb_projection2 = fb_projection2[['attributes.name','attributes.team','attributes.position','attributes.stat_type','attributes.line_score']]
            fb_projection2['attributes.line_score'] = fb_projection2['attributes.line_score'].astype(float)
            fb_projection['attributes.odds_type'] = fb_projection.groupby(['attributes.name', 'attributes.stat_type', 'attributes.odds_type']).cumcount().astype(str).radd('_').radd(fb_projection['attributes.odds_type'])
            fb_projection.loc[fb_projection['attributes.odds_type'].str.endswith('_0'), 'attributes.odds_type'] = fb_projection['attributes.odds_type'].str.replace('_0', '', regex=False)
            
            cf_pivot= fb_projection.pivot_table(values = 'attributes.line_score',index=['attributes.name','attributes.team', 'attributes.odds_type'],columns='attributes.stat_type').reset_index()
            cf_pivot['3-PT Made'] = cf_pivot.get('3-PT Made', float('NaN')) 
            cf_pivot['Blks+Stls'] = cf_pivot.get('Blks+Stls', float('NaN'))
            cf_pivot['Free Throws Made'] = cf_pivot.get('Free Throws Made', float('NaN'))
            cf_pivot['Turnovers'] = cf_pivot.get('Turnovers', float('NaN'))
            cf_pivot['Pts+Rebs'] = cf_pivot.get('Pts+Rebs', float('NaN'))
            cf_pivot['Pts+Asts'] = cf_pivot.get('Pts+Asts', float('NaN'))
            cf_pivot['Pts+Rebs+Asts'] = cf_pivot.get('Pts+Rebs+Asts', float('NaN'))
            cf_pivot['Fantasy Score'] = cf_pivot.get('Fantasy Score', float('NaN'))
            cf_pivot['Rebs+Asts'] = cf_pivot.get('Rebs+Asts', float('NaN'))   
            cf_pivot['Assists'] = cf_pivot.get('Assists', float('NaN'))  
            cf_pivot['Rebounds'] = cf_pivot.get('Rebounds', float('NaN'))  
            cf_pivot['Points'] = cf_pivot.get('Points', float('NaN'))  
            
            # cf_pivot2= fb_projection2.pivot2_table(values = 'attributes.line_score',index=['attributes.name','attributes.team'],columns='attributes.stat_type').reset_index()
            # cf_pivot2['3-PT Made'] = cf_pivot2.get('3-PT Made', float('NaN')) 
            # cf_pivot2['Blks+Stls'] = cf_pivot2.get('Blks+Stls', float('NaN'))
            # cf_pivot2['Free Throws Made'] = cf_pivot2.get('Free Throws Made', float('NaN'))
            # cf_pivot2['Turnovers'] = cf_pivot2.get('Turnovers', float('NaN'))
            # cf_pivot2['Pts+Rebs'] = cf_pivot2.get('Pts+Rebs', float('NaN'))
            # cf_pivot2['Pts+Asts'] = cf_pivot2.get('Pts+Asts', float('NaN'))
            # cf_pivot2['Pts+Rebs+Asts'] = cf_pivot2.get('Pts+Rebs+Asts', float('NaN'))
            # cf_pivot2['Fantasy Score'] = cf_pivot2.get('Fantasy Score', float('NaN'))
            # cf_pivot2['Rebs+Asts'] = cf_pivot2.get('Rebs+Asts', float('NaN'))   
            # cf_pivot2['Assists'] = cf_pivot2.get('Assists', float('NaN'))  
            # cf_pivot2['Rebounds'] = cf_pivot2.get('Rebounds', float('NaN'))  
            # cf_pivot2['Points'] = cf_pivot2.get('Points', float('NaN'))  
    except: 
             pass

    return cf_pivot