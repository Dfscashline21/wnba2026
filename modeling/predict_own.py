
import requests
import pandas as pd
import json
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from db.conn import get_db_engine
from dateutil.parser import parse
import datetime
from pytz import timezone
import numpy as np 
from pathlib import Path
import os, traceback


p = Path(__file__).parent.parent  # project root


def predict_ownership():
       
        
        url = 'https://www.linestarapp.com/DesktopModules/DailyFantasyApi/API/Fantasy/GetSalariesV5?periodId=862&site=1&sport=12'
        req = requests.get(url).json()
        
        date = req['Periods']
        periods = []
        for i in range(len(date)):
                period = date[i]
                startdate = period['StartDate']
                periodid = period['Id']
                joint = [startdate,periodid]
                periods.append(joint)
            
        period_info = pd.DataFrame(periods,columns=['Date','PeriodID'])
        period_info['Date'] = pd.to_datetime(period_info['Date']).dt.date
        period_dates =  period_info.set_index('PeriodID').to_dict()
               
        
        total_data = []
        
        for key in period_dates['Date']:
                try:
                    print(key)
                    print(period_dates['Date'][key])
                    
                    data_url = 'https://www.linestarapp.com/DesktopModules/DailyFantasyApi/API/Fantasy/GetSalariesV5?periodId=' + str(key) + '&site=1&sport=12'
                    req = requests.get(data_url).json()
                    
                    frame = []
                    
                    data =json.loads(req['SalaryContainerJson'])
                    
                    
                    for i in range(len(data['Salaries'])):
                        check = pd.DataFrame.from_records([data['Salaries'][i]])
                        frame.append(check)
                    
                    player_data = pd.concat(frame)
                    
                    own = req['Ownership']['ContestResults'][0]['OwnershipData']
                    
                    
                    ownership_list = []
                    for i in range(len(own)):
                        player_id = own[i]['PlayerId']
                        ownership = own[i]['Owned']
                        result = [player_id,ownership]
                        ownership_list.append(result)
                    
                    ownership_table = pd.DataFrame(ownership_list , columns =['PID','Ownership'])
                    
                    full_players = pd.merge(player_data, ownership_table, how='left', on='PID')
                    full_players['Ownership'] = full_players['Ownership'].fillna(0)
                    full_players['Date'] = period_dates['Date'][key]
                    total_data.append(full_players)
                except:
                    pass
        
        main = pd.concat(total_data)
        main.to_csv(p / 'wnba_historical.csv', index=False)
        
        pos_replace = {'PG':1,'SG':1,'PG/SG':1,'SF/PF':2,'PF':2}
        
        main = main.replace(pos_replace)
        main['PPD'] = main['PP'] / (main['SAL']/1000)
        main.columns
        learning_data = main[['Ceil', 'Floor','POS', 'PP', 'PPG', 'SAL', 'Ownership','PPD']]
        
        train_data, test_data = train_test_split(learning_data, test_size=0.2, random_state=42)
        
        features = ['POS', 'PPG', 'Ceil', 'Floor','PPD']
        target = 'Ownership'
        
        X_train = train_data[features]
        y_train = train_data[target]
        X_test = test_data[features]
        y_test = test_data[target]
        
        model = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
        model.fit(X_train, y_train)
        
        score = model.score(X_test, y_test)
        print(f"Model R^2 score: {score:.2f}")
        
        wnba = pd.read_csv(p / 'wnbaload.csv')
        wnba = wnba.replace(pos_replace)
        
        
        wnba.columns
        new_wnba = wnba[['Position' ,'Salary', 'AvgPointsPerGame', 'dkpts','Projection Ceil', 'Projection Floor']]
        new_wnba.columns = ['POS','SAL','PPG','PP','Ceil','Floor']
        
        new_wnba['PPD'] = new_wnba['PP'] / (new_wnba['SAL']/1000)
        X_new = new_wnba[features]
        y_pred = model.predict(X_new)
        
        new_wnba['predicted_ownership'] = y_pred
        
        result = pd.merge(wnba,new_wnba[['predicted_ownership']],left_index = True,right_index =True)
        
        new_dict = {value:key for (key,value) in pos_replace.items()}
        result['Position'] = result['Position'].replace(new_dict)
        
        result.to_csv(p / 'projection_own.csv', index=False)
        
        totalown = result['predicted_ownership'].sum()
        
        result['predicted_ownership'] = result['predicted_ownership'] / totalown *600
        
        main.to_csv(p / 'actualown.csv', index=False)

        engine = get_db_engine()
        median_sql = """select * from wnba.medians"""

        median_df = pd.read_sql(median_sql, engine).fillna(0)        

        std_sql = """select * from wnba.standard"""

        standard = pd.read_sql(std_sql, engine).fillna(0)


        dk = pd.read_csv(p / 'wnbadk.csv')

        slate = pd.merge(median_df,dk, how='left', right_on ='Name',left_on='player_name')

        slate['value'] = (slate['dkpts']/slate['Salary']) *1000

        slate.to_csv(p / 'wnbaslate.csv', index=False)
        
        topfant = slate.sort_values('dkpts', ascending=False).head(5).reset_index()
        topvalue = slate.sort_values('value', ascending = False).head(5).reset_index()
        
        dkproj = slate.sort_values('dkpts', ascending=False).head(2).reset_index()
        dkvalue = slate.sort_values('value', ascending=False).head(2).reset_index()
        
        article = "For today's slate, the top 5 projected fantasy scorers are:\n"
        for i, row in topfant.iterrows():
                player = row['player_name']
                points = round(row['dkpts'],2 )
                minutes = round(row['minutes_projection'],2)
                team = row['TeamAbbrev_y']
                value = round((row['dkpts'] / row['Salary']) *1000,2)
                salary = row['Salary']
                summary = f"{player} ${salary} ({team}): Our model has {player} projected for {points} fantasy point with a minutes projection of {minutes}. DK value: {value}"
                article += f"{i+1}. {summary}\n"
        
        print(article)
        
        valuearticle = "The top 5 DK values are:\n"
        for i, row in topvalue.iterrows():
                player = row['player_name']
                points = round(row['dkpts'],2 )
                minutes = round(row['minutes_projection'],2)
                team = row['TeamAbbrev_y']
                salary = row['Salary']
                value = round((row['dkpts'] / row['Salary']) *1000,2)
                summary = f"{player} ${salary} ({team}): Our model has {player} projected for {points} fantasy point with a minutes projection of {minutes}. DK value: {value}"
                valuearticle += f"{i+1}. {summary}\n"
        
        print(valuearticle)
        
                
        dkarticle = "Here are our top plays for today's Draftkings Slate:\n"
        for i, row in dkproj.iterrows():
                player = row['player_name']
                points = round(row['dkpts'],2 )
                minutes = round(row['minutes_projection'],2)
                team = row['TeamAbbrev_y']
                value = round((row['dkpts'] / row['Salary']) *1000,2)
                salary = row['Salary']
                summary = f"{player} ${salary} ({team}): Our model has {player} projected for {points} fantasy point with a minutes projection of {minutes}. DK value: {value}"
                dkarticle += f"{i+1}. {summary}\n"
        
        print(dkarticle)
        
                        
        dkvaluearticle = "Here are our top value plays for today's Draftkings Slate:\n"
        for i, row in dkvalue.iterrows():
                player = row['player_name']
                points = round(row['dkpts'],2 )
                minutes = round(row['minutes_projection'],2)
                team = row['TeamAbbrev_y']
                salary = row['Salary']
                value = round((row['dkpts'] / row['Salary']) *1000,2)
                summary = f"{player}.  Our model has {player} projected for {points} fantasy point with a minutes projection of {minutes}. DK value: {value}"
                dkvaluearticle += f"{i+1}. {summary}\n"
        
        print(dkvaluearticle)
        
        todaysdate = datetime.date.today()
        
        file_name = p / f"{todaysdate} WNBA Article.txt"
        
        with open(file_name, 'w') as file:
           file.write(article + valuearticle)
        
           
        file_name2 = p / f"{todaysdate} WNBA DK Article.txt"
        
        with open(file_name2, 'w') as file:
            file.write(dkarticle + dkvaluearticle)

        dklist = slate[['Position', 'Name + ID', 'Name', 'ID', 'Salary','Game Info', 'TeamAbbrev_y', 'AvgPointsPerGame','dkpts']]
        dkmain = pd.merge(dklist,standard, how='left', left_on='Name', right_on ='player_name')
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
        rawdata.to_csv(p / 'wnbaload.csv', index=False)
        

         
        projupload = pd.merge(rawdata, result[['ID','predicted_ownership']],how='left',on='ID')
        projupload['Projected Ownership'] = projupload['predicted_ownership']


        projupload =projupload[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info',
               'TeamAbbrev_y', 'AvgPointsPerGame', 'dkpts', 'player_name', 'dkstd',
               'Projection Ceil', 'Projection Floor', 'Max Exposure',
               'Projected Ownership', 'Max Deviation', 'Key', 'GPP', 'Play']]
        
        projupload.to_csv(p / 'wnbaload.csv', index=False)


        return projupload