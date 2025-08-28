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

# ''' QUERIES: 
# pm25_daily_city_avg = CREATE VIEW pm25_daily_city_avg AS
# SELECT 
#     DATE("datetimeFrom_utc") as date,
#     sensor_city as city,
#     AVG(CASE WHEN parameter_name = 'temperature' THEN value END) as avg_temp,
#     AVG(CASE WHEN parameter_name = 'pm25' THEN value END) as avg_pm25
# FROM daily_weather
# WHERE parameter_name IN ('temperature', 'pm25')
# GROUP BY DATE("datetimeFrom_utc"), sensor_city
# ORDER BY date, city;
# --------------------------

# pm25_daily_city_delta = CREATE VIEW pm25_daily_city_delta AS
# WITH daily_avg AS (
#     SELECT
#         sensor_city AS city,
#         DATE("datetimeFrom_utc") AS day,
#         AVG(value) AS avg_pm25
#     FROM daily_weather
#     WHERE parameter_name = 'pm25'
#     GROUP BY sensor_city, DATE("datetimeFrom_utc")
# ),
# ranked AS (
#     SELECT
#         city,
#         day,
#         avg_pm25,
#         ROW_NUMBER() OVER (PARTITION BY city ORDER BY day DESC) AS rn
#     FROM daily_avg
# )
# SELECT
#     t.city,
#     t.avg_pm25      AS today,
#     y.avg_pm25      AS yesterday
# FROM ranked t
# LEFT JOIN ranked y
#     ON t.city = y.city
#    AND y.rn = 2
# WHERE t.rn = 1
# ORDER BY t.city;

# temp_corr = CREATE VIEW temp_corr AS 
# SELECT 
#     DATE("datetimeFrom_utc") as date,
#     sensor_city as city,
#     AVG(CASE WHEN parameter_name = 'temperature' THEN value END) as avg_temp,
#     AVG(CASE WHEN parameter_name = 'pm25' THEN value END) as avg_pm25
# FROM daily_weather
# WHERE parameter_name IN ('temperature', 'pm25')
# GROUP BY DATE("datetimeFrom_utc"), sensor_city
# ORDER BY date, city;
# '''

# Example: fetch data
pm25_daily_city_avg = supabase.table("pm25_daily_city_avg").select("*").execute()
dailyAvg = pd.DataFrame(pm25_daily_city_avg.data)

pm25_daily_city_delta = supabase.table("pm25_daily_city_delta").select("*").execute()
dailyDelta = pd.DataFrame(pm25_daily_city_delta.data)

tempcorr = supabase.table("temp_corr").select("*").execute()
tempPm25 = pd.DataFrame(tempcorr.data)

st.set_page_config(
    page_title="Air Quality Dashboard",
    layout="wide",   # wide layout instead of default centered
    initial_sidebar_state="expanded"
)
sidebar = st.sidebar.selectbox('High Level Overview', ('High Level', 'Data Tables'))

tempPm25['date'] = pd.to_datetime(tempPm25['date']).dt.floor("D")
min_date = tempPm25['date'].min().date()
max_date = tempPm25['date'].max().date()
if 'start_date' not in st.session_state:
    st.session_state.start_date = min_date
if 'end_date' not in st.session_state:
    st.session_state.end_date = max_date


if sidebar == "High Level":
    st.markdown("<h3 style='text-align: center;'>Particle Matter - Daily Averages</h3>", unsafe_allow_html=True)
    # Subtle info expander at the top
    with st.expander("About this data"):
        st.markdown(
            """
            <b>Particle Matter or "PM"</b> is the is the number of particulates found in the air that are inhalable. 
            PM<sub>2.5</sub> specifically is the diameter of these airborne particles measuring 2.5µg/m³ or less.\n
            PM<sub>2.5</sub> is one of many measurements for which the WHO has recommended levels.\n
            This dashboard shows key air quality metrics per city:\n
            - PM<sub>2.5</sub> (µg/m³) daily average and the distance from WHO recommendation levels (15µg/m³)
            - Delta PM<sub>2.5</sub> (µg/m³) between yesterday and today, and yesterday's PM<sub>2.5</sub>
            
            ℹ️ Exceeding the WHO daily limit of 15 µg/m³ may affect respiratory health,
             and cause issues, such as asthma and bronchitis. Long term exposure can cause 
             reduced lung function in children, and premature death
            """,
        unsafe_allow_html=True
        )
    # --- KPI 1: Daily PM2.5 ---
    left_col, right_col = st.columns([1, len(dailyAvg)])
    left_col.markdown(
        "<div style='display: flex; justify-content: left; align-items: center; height: 100%;'>"
        "<h3>Daily PM<sub>2.5</sub></h3></div>", unsafe_allow_html=True
    )

    city_cols = right_col.columns(len(dailyAvg))
    for col, (_, row) in zip(city_cols, dailyAvg.iterrows()):
        value = round(row['metric_value'], 2)
        col.metric(label=f"{row['city']}", value=value, delta=round(15-value,2))

    # --- KPI 2: Δ PM2.5 ---
    left_col, right_col = st.columns([1, len(dailyDelta)])
    left_col.markdown(
        "<div style='display: flex; justify-content: left; align-items: center; height: 100%;'>"
        "<h3> Δ PM<sub>2.5</sub> </h3></div>", unsafe_allow_html=True
    )

    city_cols = right_col.columns(len(dailyDelta))
    for col, (_, row) in zip(city_cols, dailyDelta.iterrows()):
        today = round(row['today'], 2)  # yesterday’s PM2.5
        yesterday = round(row['yesterday'], 2)         # today - yesterday
        de = today-yesterday
        delta = yesterday if yesterday >= today else -yesterday
        col.metric(
            label=f"{row['city']}",
            value=f"{-de:.2f}",
            delta=delta
        )
        
    # --- KPI 3: Temperature-PM2.5 correlation ---
    # Define PM2.5 colors per city
    with st.expander("About Temperature vs. PM₂.₅"):
        st.markdown(
            """
           The below graphs show the correlation between temperature and PM<sub>2.5</sub>.\n
            Temperature to PM<sub>2.5</sub> correlation is highly dependent on environment, season, rainfall, region, and even surface pressure.\n 
            For the US cities in the graphs below, correlation between temperature and PM<sub>2.5</sub> seems to be directly related, where the higher the temperature causes 
            a higher dispersion of particulate matter.\n
            ℹ️ Higher particulate matter in the air can cause more intense and frequent wildfires, higher causes of allergens, an increase in dust storms, or droughts.
            All of which can increase PM<sub>2.5</sub>
            """,
        unsafe_allow_html=True
        )

    if "zoom_factor" not in st.session_state:
        st.session_state.zoom_factor = 20
    if "autoscale_toggle" not in st.session_state:
        st.session_state.autoscale_toggle = True
    if "start_date" not in st.session_state:
        st.session_state.start_date = min_date
    if "end_date" not in st.session_state:
        st.session_state.end_date = max_date

    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])  # equal-width columns for zoom autoscale/in/out/date toggles
    # col5, col6 = st.columns([3,1]) # equal width columns for resetting dates

    with col1: 
        autoscale = st.checkbox(
        "Autoscale Y-axis for all charts",
        key="autoscale_toggle"
    )    
    autoscale_on = st.session_state.autoscale_toggle

    with col2: 
        zoom_in = st.button("Zoom In", key="zoom_in_btn")

    with col3: 
        zoom_out = st.button("Zoom Out", key="zoom_out_btn")

    with col4:
        st.markdown("<div style='display:flex; justify-content:center;'>", unsafe_allow_html=True)
        start_date, end_date = st.date_input(
            "Select Date Range",
            value=[st.session_state.start_date, st.session_state.end_date],
            min_value=min_date,
            max_value=max_date
        )
        st.session_state.start_date = start_date
        st.session_state.end_date = end_date

    with col5:
        if st.button("Reset Filters"):
            st.session_state.start_date = min_date
            st.session_state.end_date = max_date
            st.session_state.zoom_factor = 20
            st.session_state.autoscale_toggle = True

    # Apply filtering
    filtered_tempPm25 = tempPm25[
        (tempPm25['date'] >= pd.to_datetime(st.session_state.start_date)) &
        (tempPm25['date'] <= pd.to_datetime(st.session_state.end_date))
    ]

    # Zoom logic
    if zoom_in:
        st.session_state.zoom_factor *= 0.8  # shrink range (zoom in)
    if zoom_out:
        st.session_state.zoom_factor *= 1.2  # expand range (zoom out)


    cities = ["Chicago", "Houston", "Los Angeles", "New York"]
    pm25_colors = {
        "Chicago": "green",
        "Houston": "blue",
        "Los Angeles": "purple",
        "New York": "orange"
    }
    temp_color = "red"

    # Determine global axis ranges
    pm25_min = tempPm25['avg_pm25'].min()
    pm25_max = tempPm25['avg_pm25'].max()
    temp_min = tempPm25['avg_temp'].min()
    temp_max = tempPm25['avg_temp'].max()

    # Split cities into 2 rows
    rows = [cities[:2], cities[2:]]

    for row_cities in rows:
        cols = st.columns(2)  # 2 charts per row
        for col, city in zip(cols, row_cities):
            city_data = filtered_tempPm25[filtered_tempPm25['city'] == city]
            city_data['pm25_norm'] = (city_data['avg_pm25'] - city_data['avg_pm25'].min()) / (city_data['avg_pm25'].max() - city_data['avg_pm25'].min())
            city_data['temp_norm'] = (city_data['avg_temp'] - city_data['avg_temp'].min()) / (city_data['avg_temp'].max() - city_data['avg_temp'].min())

            fig = go.Figure()

            # PM2.5 line
            fig.add_trace(
                go.Scatter(
                    x=city_data['date'],
                    y=city_data['avg_pm25'],
                    mode="lines+markers",
                    name=f"PM<sub>2.5</sub> (µg/m³)",
                    line=dict(color=pm25_colors.get(city, "black"), width=2),
                    yaxis="y1"
                )
            )

            # --- Apply global autoscale ---
            if autoscale:
                yaxis_range = None
            else:
                base_max = pm25_max  # or whatever your normal max is
                base_min = pm25_min  # optional
                yaxis_range = [base_min, base_max * st.session_state.zoom_factor]
            # yaxis_range = None if autoscale else [pm25_min, pm25_max]
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='white', dtick="D1", tickformat="%Y-%m-%d", tickangle=-30)
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='white', title_text="PM2.5 (µg/m³)", range=yaxis_range)  # PM2.5 axis

            # Add temperature as a second y-axis by assigning it directly in the trace
            fig.add_trace(
                go.Scatter(
                    x=city_data['date'],
                    y=city_data['avg_temp'],
                    mode="lines+markers",
                    name="Temperature (°C)",
                    line=dict(color=temp_color, width=2, dash="dot"),
                    yaxis="y2"
                )
            )

            # Define the second y-axis in layout
            fig.update_layout(
                title=dict(
                    text=f"{city} - PM<sub>2.5</sub> v. Temperature",
                    x=0.5,                # 0 = left, 0.5 = center, 1 = right
                    xanchor='center',
                    yanchor='top',
                    font=dict(
                        size=16,          # increase the font size
                        family="Arial, sans-serif",
                        color="black"
                    )
                ),
                plot_bgcolor='#f8f8f8',  # behind the lines / inside the axes
                paper_bgcolor='#f8f8f8',  # figure background outside the axes
                xaxis=dict(title="Date"),
                hovermode="x unified",
                width=900,   # overall figure width
                height=500,   # height of the plot
                yaxis2=dict(
                    title="Temperature (°C)",
                    overlaying="y",
                    side="right",
                    range=[temp_min, temp_max]
                ),
                legend=dict(
                    x=1.05,        # move it to the right outside the plot
                    y=1,           # top of the plot
                    xanchor='left', 
                    yanchor='top',
                    orientation='v',
                )
            )

            # Place chart in the column
            col.plotly_chart(fig, use_container_width=True, key=f"{city}_chart")


if sidebar == 'Data Tables':
    st.write(dailyAvg)
    st.write(dailyDelta)
    st.write(tempPm25)