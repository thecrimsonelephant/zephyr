import pandas as pd
import altair as alt
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load env vars
load_dotenv()
pg_password = os.getenv("PGPWD")

# Connect to Postgres
# st.write(f"This graph shows each city's particulate matter (PM) in the air with a size of 2.5 micrometers. Exceeding the WHO's limit of 15µm/day can cause respiratory issues, such as asthma and bronchitis. Long term exposure can cause reduced lung function in children, and premature death")
engine = create_engine(f'postgresql://postgres:{pg_password}@localhost:5432/postgres')

# Query daily avg PM2.5 per city
query = """
    SELECT 
        sensor_city,
        DATE("datetimeFrom (UTC)") AS date,
        AVG(value) AS pm25_daily_avg
    FROM public.daily_weather
    WHERE parameter_name = 'pm25'
    GROUP BY sensor_city, DATE("datetimeFrom (UTC)")
    ORDER BY date
"""
df = pd.read_sql(query, engine)
df['date'] = pd.to_datetime(df['date'])

# -----------------------------
# Helper: Convert PM2.5 to AQI
# -----------------------------

def pm25_to_aqi(pm25):
    """Convert PM2.5 µg/m³ to AQI using EPA breakpoints"""
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ]
    for c_low, c_high, i_low, i_high in breakpoints:
        if c_low <= pm25 <= c_high:
            return ((i_high - i_low) / (c_high - c_low)) * (pm25 - c_low) + i_low
    return None

# add AQI column
df["aqi_daily_avg"] = df["pm25_daily_avg"].apply(pm25_to_aqi)

# WHO daily PM2.5 limit
WHO_LIMIT = 15
who_df = pd.DataFrame({
    "sensor_city": ["WHO Limit"] * len(df['date'].unique()),
    "date": df['date'].unique(),
    "pm25_daily_avg": [WHO_LIMIT] * len(df['date'].unique())
})
plot_df = pd.concat([df, who_df])

# color mapping
color_scale = alt.Scale(
    domain=["Houston", "Los Angeles", "Chicago", "New York", "WHO Limit"],
    range=["red", "yellow", "lightblue", "orange", "purple"]
)

# -----------------------------
# Chart 1: Daily PM2.5
# -----------------------------
pm25_chart = (
    alt.Chart(plot_df)
    .mark_line(point=True)
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45, tickCount="day")),
        y=alt.Y("pm25_daily_avg:Q", title="Daily Avg PM2.5 (µg/m³)"),
        color=alt.Color("sensor_city:N", scale=color_scale, title="Legend"),
        tooltip=["date:T", "sensor_city:N", "pm25_daily_avg:Q"]
    )
    .properties(title="Particle Matter (PM2.5µg/m³) per City")
)

# -----------------------------
# Chart 2: Daily AQI
# -----------------------------
color_scale2 = alt.Scale(
    domain=["Houston", "Los Angeles", "Chicago", "New York"],
    range=["red", "yellow", "lightblue", "orange"]
)

aqi_chart = (
    alt.Chart(df)
    .mark_line(point=True)
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45, tickCount="day")),
        y=alt.Y("aqi_daily_avg:Q", title="Daily Avg AQI"),
        color=alt.Color("sensor_city:N", scale=color_scale2, title="Legend"),
        tooltip=["date:T", "sensor_city:N", "aqi_daily_avg:Q"]
    )
    .properties(title="Air Quality Index per City")
)

# -----------------------------
# Show in Streamlit
# -----------------------------
st.title("Air Quality Trends")

# Side by side
col1, col2 = st.columns(2)
with col1:
    st.altair_chart(pm25_chart, use_container_width=True)
with col2:
    st.altair_chart(aqi_chart, use_container_width=True)
