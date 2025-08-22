# NOAA Weather Data ETL Pipeline

**Extract, Transform, Load (ETL)** pipeline for automated ingestion and processing of NOAA time-series weather data—focused on temperature, precipitation, and derived indices (Heat Index, Flood Risk) by U.S. state.

---

##  Why It Matters
- Helps **agriculture** forecast yield stress due to heat/frost.
- Informs **energy demand modeling**, **logistics routing**, and **risk assessment** in insurance.
- Provides structured weather data for **time-series analytics**, forecasting, ML, and dashboarding.

---

##  Pipeline Overview

| Stage     | Description                                                                                                           |
|-----------|-----------------------------------------------------------------------------------------------------------------------|
| **Extract** | Downloads NOAA weather data using [ https://www.noaa.gov/ ] from selected weather stations.                           |
| **Transform** | Processes raw data via pandas—cleans, computes indices (Heat Index, Flood Risk), generates CSVs.                      |
| **Load**    | Stores processed data into a local (or cloud-compatible) MySQL database with proper indexing for time-series queries. |

---

##  Getting Started

### Prerequisites
- Python 3.8+  
- MySQL server accessible locally or remotely  
- Basic knowledge of NOAA data formats (e.g. GHCN-Daily)

### Installation
1. Clone the repo:
   ```bash
   git clone https://github.com/glorymary96/NOAA-Weather-Data.git
   cd NOAA-Weather-Data
   ```
2. Install dependencies
    ```bash
    pip install -r requirements.txt 
   ```
3. Edit ```PARAMS.py```
   Ensure MySQL credentials and target database exist and are accessible.

4. Run the Pipeline
   ``` bash
   python main.py
   ```
   This will:

   - Extract weather data for specified states and date range.

   - Transform it and save as CSV in output/.

   - Load transformed data into the MySQL database.


5. Project Structure
```
├── ExtractData.py       # Fetches NOAA data
├── WeatherTransform.py  # Cleans, computes indices, outputs CSV
├── LoadData.py          # Inserts data into MySQL
├── Regions.py           # Maps states/counties to station metadata
├── sqlactions.py        # SQL utility functions
├── main.py              # Orchestrates ETL flow
├── PARAMS.py            # For configuration
├── requirements.txt     # Required Python packages
├── README.md            # Documentation (you’re here!)
```
The gridded weather data from NOAA is analyzed within each state of US. The code can be easily extented for other counties as well. 
We have mainly considered temperature and precipitation related factors, considering the Heat Index, Flood Risk Index and Frost Risk Index 
patterns. These patterns mainly define the health of the crop.

   

