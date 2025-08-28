import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import create_engine
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime as dt, timedelta as td
from supabase import create_client
from dotenv import load_dotenv
import os 

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")
print(SUPABASE_URL)
print(SUPABASE_API_KEY)

st.title("Weather Dashboard")

supabase = create_client(supabase_url=SUPABASE_URL, supabase_key=SUPABASE_API_KEY)

# Example: fetch data
data = supabase.table("daily_weather").select("*").execute()
df = pd.DataFrame(data.data)
st.dataframe(df)