from ExtractData import get_data
from PARAMS import LOG, GET_COUNTRIES, YEARS, FILES_DIR, clean_weather_db
from Regions import regions
import LoadData
import xarray as xr

from PARAMS import *

if __name__=='__main__':

    # Loops over the countries to be included for analysis
    for country in GET_COUNTRIES:
        # Loops over the years

        for yr in YEARS:
            dict_weather = []
            #Get the data for each of the metrics (max temp, min temp and precipitation)

            for metric in ['tmax', 'tmin', 'precip']:
                get_data(metric, yr)
                LOG(metric + ' - Data download completed')

            # Load the data for transformation
            tmax = xr.open_dataset(FILES_DIR + 'tmax.nc').to_dataframe().reset_index()
            tmin = xr.open_dataset(FILES_DIR + 'tmin.nc').to_dataframe().reset_index()
            precip = xr.open_dataset(FILES_DIR + 'precip.nc').to_dataframe().reset_index()

            # Transform the data for necessary analysis and get the clean data
            for reg in regions:
                dict_weather.append(reg.build_measures_NOAA(tmax, yr, 'tmax', 'TMAX'))
                dict_weather.append(reg.build_measures_NOAA(tmin, yr, 'tmin', 'TMIN'))
                dict_weather.append(reg.build_measures_NOAA(precip, yr, 'precip','PRCP'))

            # Concatenate the data of different metrics and regions
            LOG(' - Concatenating the metrics')
            LoadData.create_csv(dict_weather, FILES_DIR +'weather_data')

            # Load the data into MYSQL (local)
            LoadData.load_csv(FILES_DIR + 'weather_data.csv', TABLE_DEF, CONN_PARAMS)
            LOG(' - Uploading the historical data')

    # Clean the database in case of dummy data, to keep the DB clean
    clean_weather_db()
