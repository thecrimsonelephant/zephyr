# Zephyr
zeph·yr (noun)

/ˈzefər/

Definition: a soft gentle breeze ([Webster](https://www.merriam-webster.com/dictionary/zephyr))

---

This project, Zephyr, strives to be an end-to-end data pipeline and dashboard that blends air quality and weather data into executive-ready insights.  
Built with Python and free public APIs, it demonstrates the full ETL journey: ingestion → transformation → storage → visualization.

[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/) 
[![Streamlit](https://img.shields.io/badge/Streamlit-Live%20Demo-red)](https://streamlit.io/) 
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📊 Project Overview
- Problem: Air quality affects public health and insights often live in siloed datasets.  
- Solution: Zephyr ingests air quality and weather data, cleans and combines them, and delivers executive KPIs and interactive dashboards.  
- Goal: Showcase the ability to build end-to-end data pipelines in Python.

---

## 🔧 Tech Stack
- Data Sources:  
  - [OpenAQ API](https://docs.openaq.org/) → air quality  
  - [Open-Meteo API](https://open-meteo.com/) → weather 
- Ingestion: Python (`requests`, `pandas`)   
- Storage:
  - PostgreSQL → local storage 
  - Supabase → online public storage & visualization ease
- Visualization: Streamlit Cloud
- Automation: 
  - GitHub Actions (daily pipeline runs)  
  - Apache Airflow

---

## 📂 Repo Structure
```
zephyr/
├── pipeline/ # API ingestion + ETL scripts
├── data/ # sample data extracts (CSV)
├── dashboard/ # Streamlit dashboard app
├── .github/workflows # GitHub Actions workflows
├── ingestion.py # ingesting data into Postgres local
└── README.md # project documentation
```

---

## 🚀 Features
- ✅ Multi-city air quality ingestion (PM2.5, PM10, CO, O₃, etc.)  
- ✅ Weather data integration (temperature, humidity, wind speed)  
- ✅ Data cleaning & transformation pipeline  
- ✅ KPIs: WHO exceedance %, city rankings, correlations  
- ✅ Executive dashboard with charts, filters, and maps
  
---

## ⚡ Completions (Current and Planned)
- [X] OpenAQ API
- [X] Open-Meteo API
- [X] Postgres local & Supabase Data Storage
- [X] Streamlit Cloud
- [ ] Automation
---

## 📈 Example Dashboard
*TBD :)*

- [ ] KPI cards (daily AQI, delta, difference)
- [ ] Scatter or line plots (PM<sub>2.5</sub> vs temperature/wind)  
- [ ] Map visualization of AQI across cities  
