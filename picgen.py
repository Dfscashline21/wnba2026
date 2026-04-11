# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 20:01:04 2025

@author: trent
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import os

save_folder = "c:/users/trent/wnba/pics"


os.makedirs(save_folder, exist_ok=True)

players = pd.read_csv('C:/Users/trent/WNBA/playerids.csv')

def build_url(row):
    return f"https://cdn.wnba.com/headshots/wnba/latest/260x190/{row['player_id']}.png"

players["img_url"] = players.apply(build_url, axis=1)

for index, row in players.iterrows():
    image_url = row["img_url"]
    player_id = row["player_id"]
    name =row["player_name"]
    
    response = requests.get(image_url)
    if response.status_code == 200:
        file_path = os.path.join(save_folder, f"{player_id}_{name}.png")
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"✅ Saved: {file_path}")
    else:
        print(f"❌ Failed for {player_id} ({name}) - Status code: {response.status_code}")
