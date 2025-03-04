## ETL Pipeline for weather data from NOAA 

Weather data, especially in time series form, is crucial across industries like agriculture, energy, logistics, and insurance. 
It helps forecast trends, optimize operations, and manage risks. For instance, time series data aids in predicting crop yields, 
energy demand, delivery routes, and assessing insurance risks. By analyzing past weather patterns, businesses can make informed 
decisions to improve efficiency and reduce disruptions.

This repository contains an ETL (Extract, Transform, Load) pipeline designed to process and analyze weather data. The pipeline 
extracts weather data from National Oceanic and Atmospheric Administration (NOAA), transforms it into a structured format suitable for analysis, and loads it into a 
database for further use. The system is built to handle large datasets, enabling efficient time series analysis for forecasting 
and insights.

1. Extract - Extracted data from NOAA
2. Transform - Transformed the data using pandas into a structured csv file
3. Load - Loaded the data into a MySQL local database

The gridded weather data from NOAA is analyzed within each state of US. The code can be easily extented for other counties as well. 
We have mainly considered temperature and precipitation related factors, considering the Heat Index, Flood Risk Index and Frost Risk Index 
patterns. These patterns mainly define the health of the crop.
