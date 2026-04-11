# -*- coding: utf-8 -*-
"""
Updated CBB Streamlit app using Supabase Storage instead of S3
Replaces S3 functionality with Supabase Storage

@author: trent
"""

import streamlit as st
import pandas as pd
import numpy as np
import datetime
import plotly.express as px
from supabase_storage import supabase_storage  # Replace boto3 with Supabase
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(layout="wide")

@st.cache_data
def data_pull(file):
    """Pull data from Supabase Storage instead of S3"""
    try:
        # Download from Supabase Storage
        df = supabase_storage.download_csv('cbbdata2023', file)
        if df is not None:
            logger.info(f"✅ Successfully loaded {file} from Supabase Storage")
            return df
        else:
            st.error(f"❌ Failed to load {file} from Supabase Storage")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error loading {file}: {e}")
        return pd.DataFrame()

# Load data from Supabase Storage
playerproj = data_pull('playerprojections.csv')
if not playerproj.empty:
    playerproj = playerproj.drop_duplicates(subset='player')

cbb = data_pull('underdog.csv')
cf_pivot = data_pull('prizepicks.csv')
proppiv = data_pull('caesars.csv')
nojuice = data_pull('nojuice.csv')
projectionset = data_pull('projections.csv')
df = data_pull('logs.csv')
gamebetting = data_pull('betting.csv')

# ... rest of existing code remains the same ...

def save_data_to_supabase(df: pd.DataFrame, file_name: str):
    """Save data to Supabase Storage"""
    try:
        success = supabase_storage.upload_csv(df, 'cbbdata2023', file_name, public=True)
        if success:
            st.success(f"✅ Successfully saved {file_name} to Supabase Storage")
        else:
            st.error(f"❌ Failed to save {file_name} to Supabase Storage")
    except Exception as e:
        st.error(f"❌ Error saving {file_name}: {e}")

# Add download button that saves to Supabase
if st.button('Save Current Data to Supabase'):
    if 'filtered_df' in locals():
        save_data_to_supabase(filtered_df, f'filtered_data_{datetime.date.today()}.csv')

# ... rest of existing Streamlit app code ...
