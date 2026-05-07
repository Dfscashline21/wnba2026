    # -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 16:38:48 2025

@author: trent
"""

from db.conn import get_db_engine
import pandas as pd
from datetime import date
from sqlalchemy import text

def trifecta_tweet():
    engine = get_db_engine()
    
    
    
    # plays_sql = """        
    # with filtered as (            
    # SELECT player_name, "attributes.odds_type", 'Points' AS stat, "Points" as line,pointsover AS over_value
    # FROM wnba.ppovers
    # WHERE pointsover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Pts+Rebs' AS stat,"Pts+Rebs" as line, prover AS over_value
    # FROM wnba.ppovers
    # WHERE prover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Pts+Asts' AS stat,"Pts+Asts", paover AS over_value
    # FROM wnba.ppovers
    # WHERE paover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Pts+Rebs+Asts' AS stat,"Pts+Rebs+Asts", parover AS over_value
    # FROM wnba.ppovers
    # WHERE parover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Rebounds' AS stat,"Rebounds", rebover AS over_value
    # FROM wnba.ppovers
    # WHERE rebover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Assists' AS stat, "Assists",astover AS over_value
    # FROM wnba.ppovers
    # WHERE astover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Rebs+Asts' AS stat,"Rebs+Asts", raover AS over_value
    # FROM wnba.ppovers
    # WHERE raover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Fantasy Score' AS stat,"Fantasy Score", fantover AS over_value
    # FROM wnba.ppovers
    # WHERE fantover >= 0.65
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", '3-PT Made' AS stat,"3-PT Made", threesover AS over_value
    # FROM wnba.ppovers
    # WHERE threesover >= 0.65
    # union all
    # SELECT player_name, "attributes.odds_type", 'Points' AS stat, "Points" as line,pointsover AS over_value
    # FROM wnba.ppovers
    # WHERE pointsover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Pts+Rebs' AS stat,"Pts+Rebs" as line, prover AS over_value
    # FROM wnba.ppovers
    # WHERE prover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Pts+Asts' AS stat,"Pts+Asts", paover AS over_value
    # FROM wnba.ppovers
    # WHERE paover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Pts+Rebs+Asts' AS stat,"Pts+Rebs+Asts", parover AS over_value
    # FROM wnba.ppovers
    # WHERE parover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Rebounds' AS stat,"Rebounds", rebover AS over_value
    # FROM wnba.ppovers
    # WHERE rebover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Assists' AS stat, "Assists",astover AS over_value
    # FROM wnba.ppovers
    # WHERE astover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Rebs+Asts' AS stat,"Rebs+Asts", raover AS over_value
    # FROM wnba.ppovers
    # WHERE raover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", 'Fantasy Score' AS stat,"Fantasy Score", fantover AS over_value
    # FROM wnba.ppovers
    # WHERE fantover <= 0.35
    # UNION ALL
    # SELECT player_name, "attributes.odds_type", '3-PT Made' AS stat,"3-PT Made", threesover AS over_value
    # FROM wnba.ppovers
    # WHERE threesover <= 0.35)
    # select distinct on ("attributes.odds_type")
    # player_name, "attributes.odds_type" ,stat,line,over_value
    # from filtered 
    # WHERE "attributes.odds_type" IN ('goblin', 'demon', 'standard')
    # order by "attributes.odds_type" , over_value desc
                
    #             """
                
    plays_sql = text("""     with filtered as (            
    SELECT player_name, "attributes.odds_type", 'Points' AS stat, "Points" as line,pointsover AS over_value
    FROM wnba.ppovers
    WHERE pointsover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Pts+Rebs' AS stat,"Pts+Rebs" as line, prover AS over_value
    FROM wnba.ppovers
    WHERE prover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Pts+Asts' AS stat,"Pts+Asts", paover AS over_value
    FROM wnba.ppovers
    WHERE paover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Pts+Rebs+Asts' AS stat,"Pts+Rebs+Asts", parover AS over_value
    FROM wnba.ppovers
    WHERE parover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Rebounds' AS stat,"Rebounds", rebover AS over_value
    FROM wnba.ppovers
    WHERE rebover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Assists' AS stat, "Assists",astover AS over_value
    FROM wnba.ppovers
    WHERE astover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Rebs+Asts' AS stat,"Rebs+Asts", raover AS over_value
    FROM wnba.ppovers
    WHERE raover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Fantasy Score' AS stat,"Fantasy Score", fantover AS over_value
    FROM wnba.ppovers
    WHERE fantover >= 0.65
    UNION ALL
    SELECT player_name, "attributes.odds_type", '3-PT Made' AS stat,"3-PT Made", threesover AS over_value
    FROM wnba.ppovers
    WHERE threesover >= 0.65
    union all
    SELECT player_name, "attributes.odds_type", 'Points' AS stat, "Points" as line,pointsover AS over_value
    FROM wnba.ppovers
    WHERE pointsover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Pts+Rebs' AS stat,"Pts+Rebs" as line, prover AS over_value
    FROM wnba.ppovers
    WHERE prover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Pts+Asts' AS stat,"Pts+Asts", paover AS over_value
    FROM wnba.ppovers
    WHERE paover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Pts+Rebs+Asts' AS stat,"Pts+Rebs+Asts", parover AS over_value
    FROM wnba.ppovers
    WHERE parover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Rebounds' AS stat,"Rebounds", rebover AS over_value
    FROM wnba.ppovers
    WHERE rebover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Assists' AS stat, "Assists",astover AS over_value
    FROM wnba.ppovers
    WHERE astover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Rebs+Asts' AS stat,"Rebs+Asts", raover AS over_value
    FROM wnba.ppovers
    WHERE raover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", 'Fantasy Score' AS stat,"Fantasy Score", fantover AS over_value
    FROM wnba.ppovers
    WHERE fantover <= 0.35 and "attributes.odds_type" like 'standard%'
    UNION ALL
    SELECT player_name, "attributes.odds_type", '3-PT Made' AS stat,"3-PT Made", threesover AS over_value
    FROM wnba.ppovers
    WHERE threesover <= 0.35 and "attributes.odds_type" like 'standard%')
    select distinct on ("attributes.odds_type")
    player_name, "attributes.odds_type" ,stat,line,over_value
    from filtered 
    WHERE "attributes.odds_type" like any (:patterns)
    order by "attributes.odds_type" , over_value desc
                """).bindparams(patterns =['goblin%', 'demon%', 'standard%'])                
    
    plays_df = pd.read_sql(plays_sql, engine).fillna(0)
    
    
    label_map = {
        'standard': 'Standard 👔',
        'goblin': 'Goblin 👺',
        'demon': 'Demon 😈',
        'standard_1': 'Standard 👔',
        'goblin_1': 'Goblin 👺',
        'demon_1': 'Demon 😈',
        'standard_2': 'Standard 👔',
        'goblin_2': 'Goblin 👺',
        'demon_2': 'Demon 😈',
        'standard_3': 'Standard 👔',
        'goblin_3': 'Goblin 👺',
        'demon_3': 'Demon 😈'
    }
    
    # Build the tweet
    date_str = date.today().strftime("%m/%d")
    tweet = f"WNBA Trifecta Picks 🔮 {date_str}\n\n"
    
    for _, row in plays_df.iterrows():
        label = label_map.get(row['attributes.odds_type'], 'Unknown')
        if row['over_value'] >= .65:
            tweet += f"{label}\n{row['player_name']} o{row['line']} {row['stat']}\n\n"
        else:
            tweet += f"{label}\n{row['player_name']} u{row['line']} {row['stat']}\n\n"
    tweet += "If you enjoy the freemium data and are tired of guessing, Join Us Now at https://fastbreakfantasyhoops.com/membership-join/ \n\n"
    tweet += "#WNBA #props #propbetting #playerprops #gamblingtwitter #DraftKings #PrizePicks"
    
    print(tweet)
    
    # best_sql = """        
    # select * from nba.prizepicksrec
                
    #             """
    # best_df =pd.read_sql(best_sql, engine).fillna(0)
    
    # prem = f"NBA Model Picks 🔮 {date_str}\n\n"
    # for _, row in best_df.iterrows():
    #     label = label_map.get(row['odds_type'], 'Unknown')
    #     prem += f"{label}\n{row['player_name']} o{row['line']} {row['stat']}\n\n"
        
    # prem += "#PrizePicks #NBA #GamblingTwitter"
        
    # print(prem)