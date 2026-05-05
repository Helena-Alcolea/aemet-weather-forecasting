# AEMET Climate Forecasting

Multi-horizon regression models for predicting extreme temperature and 
precipitation events using historical SYNOP station data from AEMET 
(Spain's State Meteorological Agency).

**Status:** In progress — Data pipeline complete. EDA upcoming.

---

## Overview

This project builds and evaluates ML/DL models to forecast daily maximum 
temperature, minimum temperature and precipitation at **1, 3 and 7-day 
horizons** across Spain's SYNOP meteorological network.

The forecasting task is framed as a multivariate regression problem over 
meteorological time series. Both classical ML approaches and deep learning 
sequence models are developed and compared, using a rich feature set that 
includes lagged climate variables, atmospheric pressure, relative humidity, 
solar radiation, altitude and geographic coordinates.

---

## Project Phases

**Phase 1 — EDA & Station Selection** *(upcoming)*  
Exploratory analysis of the full dataset. Selection of stations with 
continuous records since 1991 and less than 10% missing data, following 
WMO quality standards. Spatial and temporal characterisation of temperature 
and precipitation across Spain's SYNOP network.

**Phase 2 — ML Baseline** *(upcoming)*  
Training and evaluation of XGBoost, LightGBM and Random Forest regressors 
for 1, 3 and 7-day forecast horizons. Evaluation metrics: RMSE, MAE and R². 
Feature importance analysis to identify the most relevant predictors at 
each horizon.

**Phase 3 — DL Sequence Models** *(upcoming)*  
Development of LSTM, GRU and CNN architectures for sequence-based 
forecasting. Systematic comparison against ML baselines across all 
target variables and forecast horizons.

---

## Data

| | |
|---|---|
| **Agency** | AEMET — Agencia Estatal de Meteorología |
| **Network** | SYNOP (~300 stations across Spain) |
| **API** | AEMET OpenData API |
| **Target variables** | Daily maximum temperature · Daily minimum temperature · Daily precipitation |
| **Feature variables** | Lagged tmax, tmin, prec · Atmospheric pressure · Relative humidity · Solar radiation · Wind speed · Altitude · Coordinates |
| **Coverage** | 1991–present (stations with continuous records) |
| **Volume** | 3M+ daily records |

---

## Data Pipeline

A production-grade pipeline was built to extract and validate historical 
records from the AEMET API prior to modelling:

- Incremental extraction per station with adaptive retry logic and rate 
  limit management
- Gap auditing: detection and quantification of missing intervals
- Historical backfill: automated recovery of missing records with 
  multi-attempt verification
- PostgreSQL database storing 3M+ validated daily records

Pipeline code available in `data_ingestion/` and `data_audit/`.

---

## Station Selection Criteria

Stations are included if they meet all of the following:

- Continuous records since **1991**, covering the current AEMET 
  climatological reference period (1991–2020)
- Minimum record span of **30 years** (WMO standard)
- **Less than 10% missing data** over the full record span, following 
  the completeness threshold used by the WMO for centennial station 
  recognition

---

## Tech Stack

| Component | Technology |
|---|---|
| Data extraction & processing | Python · Pandas · NumPy |
| Database | PostgreSQL |
| ML models | Scikit-learn · XGBoost · LightGBM |
| DL models | TensorFlow · Keras |
| Visualisation | Matplotlib · Seaborn |
| Version control | Git · GitHub |

---

## Author

Helena Alcolea Ruiz · Physicist (BSc + MSc in Complex Systems) · 
Data Scientist · [LinkedIn](#) · [GitHub](#)
