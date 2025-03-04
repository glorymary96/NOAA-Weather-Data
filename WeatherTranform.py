import json
from PARAMS import WORKING_DIR, LOG, DATE_NOW
import pandas as pd
import os
from typing import List, Union
import numpy as np
import geopandas as gpd
from shapely import geometry
from shapely.geometry import Point
import datetime as dt


class TransformWeatherRegions:
    def __init__(self,
                 country: str,
                 state:str,
                 region: str,
                 crop:str,
                 start_month:str,
                 end_month:str
                 ):
        self.country = country
        self.state = state
        self.region = region
        self.crop = crop
        self.start_month = start_month
        self.end_month = end_month
        self.polygon_region = self.getWeatherRegion(self.country,  self.state)

    def build_measures_NOAA(self,
                            data: pd.DataFrame,
                            year:int,
                            metric:str,
                            weather_type:str)-> pd.DataFrame:

        """
        Builds weather data based on the weather type and metric for a given region and year.
        Args:
            data (pd.DataFrame): The weather data to process.
            year (int): The year of interest.
            metric (str): The weather metric (e.g., temperature, precipitation).
            weather_db (WeatherType): The type of weather data (e.g., TMAX, TMIN, PRCP).
        Returns:
            pd.DataFrame: The processed weather data.
        """

        LOG('Building Weather for ' + self.country + ' - ' + self.state + ' - ' + self.region)

        # Filter the points inside the polygon
        df_inside = self.get_coordinates_within_region(self.polygon_region)

        # Ensure longitude is standardized between -180 and 180 degrees
        data['lon'] = abs(data['lon']) - 180
        #print(df_inside.head(), data.head())
        data = data.merge(df_inside[['lat', 'lon']], on=['lat', 'lon'], how='inner')
        data.rename(columns={metric: 'value', 'time': 'Date'}, inplace=True)
        data['value'] = data['value'].fillna(0)
        data['Date'] = pd.to_datetime(data['Date'])

        if weather_type == 'PRCP':
            df_final = self._get_precip_based_metrics(year, data, df_inside)
        elif weather_type == 'TMAX':
            df_final = self._get_max_temp_based_metrics(year, data, df_inside)
        elif weather_type == 'TMIN':
            df_final = self._get_min_temp_based_metrics(year, data, df_inside)

        return df_final


    def _get_min_temp_based_metrics(self,
                                    yr:int,
                                    data:pd.DataFrame,
                                    df_inside:pd.DataFrame,
                                    ) -> pd.DataFrame:
        data = data.set_index(['Date', 'lat', 'lon'])
        mean_temp = list(data.groupby(level=[0])['value'].mean().values)

        all_dates = pd.date_range(start=data.index.get_level_values('Date').min(),
                                  end=data.index.get_level_values('Date').max())
        temp_metrics = {
            'FRI0': data['value'] < 0,
        }

        exceedance_ratios = {metric: self.compute_exceedance_ratio(data, df_inside, all_dates, condition) for
                             metric, condition in
                             temp_metrics.items()}

        df_list = [
            self.create_dataframe('TMIN', mean_temp, DATE_NOW, all_dates)
        ]
        df_list += [
            self.create_dataframe(metric, ratio, DATE_NOW, all_dates)
            for metric, ratio in exceedance_ratios.items()
        ]

        # Concatenate all DataFrames
        return pd.concat(df_list, axis=0)

        return df_final

    def _get_max_temp_based_metrics(self,
                                    yr:int,
                                    data: pd.DataFrame,
                                    df_inside: pd.DataFrame
                                    )-> pd.DataFrame:

        data = data.set_index(['Date', 'lat', 'lon'])
        mean_temp = list(data.groupby(level=[0])['value'].mean().values)

        all_dates = pd.date_range(start=data.index.get_level_values('Date').min(),
                                  end=data.index.get_level_values('Date').max())

        temp_metrics = {
            'HI35': data['value'] > 35,
            'HI33': data['value'] > 33,
            'HI40': data['value'] > 40,
        }

        exceedance_ratios = {metric: self.compute_exceedance_ratio(data, df_inside, all_dates, condition) for
                             metric, condition in
                             temp_metrics.items()}
        df_list = [
            self.create_dataframe('TMAX', mean_temp, DATE_NOW, all_dates)
        ]
        df_list += [
            self.create_dataframe(metric, ratio, DATE_NOW, all_dates)
            for metric, ratio in exceedance_ratios.items()
        ]

        # Concatenate all DataFrames
        return pd.concat(df_list, axis=0)


    def _get_precip_based_metrics(self,
                                  yr: int,
                                  data:pd.DataFrame,
                                  df_inside:pd.DataFrame
                                  )-> pd.DataFrame:
        """
        Computes precipitation-based metrics, including rolling precipitation sums and exceedance ratios.

        Args:
            yr (int): The target year for filtering data.
            data (pd.DataFrame): The precipitation dataset containing 'lat', 'lon', 'Date', and 'value'.
            df_inside (pd.DataFrame): A DataFrame representing relevant points for normalization.
            DATE_NOW (dt.datetime): Current timestamp for metadata.

        Returns:
            pd.DataFrame: A combined DataFrame containing precipitation-related metrics.
        """
        data['value'] = data['value'].apply(lambda x: max(0, x))

        # Compute rolling precipitation sums
        rolling_metrics = {
            'rolling_precip_sum_40': ('40D', 40),
            'rolling_precip_sum_10': ('10D', 10),
            'rolling_precip_sum_5': ('5D', 5),
        }

        for metric_name, (window, min_periods) in rolling_metrics.items():
            rolling_df = self._calculate_rolling_sum(data, window, min_periods, metric_name)
            data = pd.merge(data, rolling_df, on=['lat', 'lon', 'Date'])

        # Filter data for the target year
        data = data.loc[data['Date'].dt.year == yr].reset_index(drop=True)
        data = data.set_index(['Date', 'lat', 'lon'])

        # Compute mean precipitation per date
        mean_precip = list(data.groupby(level=[0])['value'].mean().values)

        all_dates = pd.date_range(start=data.index.get_level_values('Date').min(),
                                  end=data.index.get_level_values('Date').max())

        precip_metrics = {
            'PL40': data['rolling_precip_sum_40'] < 40,
            'PG100': data['rolling_precip_sum_10'] > 100
        }

        exceedance_ratios = {metric: self.compute_exceedance_ratio(data, df_inside, all_dates, condition) for metric, condition in
                             precip_metrics.items()}

        # Build DataFrames for each metric
        df_list = [
            self.create_dataframe('PRCP', mean_precip, DATE_NOW, all_dates)
        ]
        df_list += [
            self.create_dataframe(metric, ratio, DATE_NOW, all_dates)
            for metric, ratio in exceedance_ratios.items()
        ]

        # Concatenate all DataFrames
        return pd.concat(df_list, axis=0)

    # Compute exceedance ratios for precipitation thresholds
    def compute_exceedance_ratio(self,
                                 data: pd.DataFrame,
                                 df_inside:pd.DataFrame,
                                 all_dates:List[dt.datetime],
                                 condition: pd.Series
                                 ) -> List[float]:
        num_points_per_date = data.loc[condition].groupby(level=0).size()
        return (num_points_per_date.reindex(all_dates, fill_value=0) / len(df_inside)).tolist()

    def create_dataframe(self,
            str_weather_db: str,
            metric_val: List[Union[int, float]],
            DATE_NOW: dt.datetime,
            all_dates: List[dt.datetime]
    ) -> pd.DataFrame:
        """
        Builds a DataFrame containing weather-related data with metadata.

        Args:
            yr (int): The year for which data is being generated.
            str_weather_db (str): The type of weather database (e.g., temperature, precipitation).
            metric_val (List[Union[int, float]]): List of metric values corresponding to dates.
            DATE_NOW (dt.datetime): Timestamp indicating the current date.
            country (str): Country identifier.
            state (str): State identifier.
            region (str): More granular region identifier.
            crop_name (str): Crop type associated with the data.
            all_dates (List[dt.datetime]): List of datetime objects representing the dates.

        Returns:
            pd.DataFrame: A DataFrame containing metric values, corresponding dates, and metadata.
        """
        # Create the initial DataFrame with metric values
        df = pd.DataFrame({'value': metric_val})

        # Ensure 'all_dates' matches the length of 'metric_val'
        if len(df) != len(all_dates):
            raise ValueError("Mismatch between metric_val length and all_dates length.")

        # Assign the date values
        df['date'] = all_dates

        # Add metadata columns
        df['ts'] = DATE_NOW
        df['country'] = self.country
        df['state'] = self.state
        df['region'] = self.region
        df['type'] = str_weather_db
        df['crop'] = self.crop

        return df

    def _calculate_rolling_sum(self,
                               data:pd.DataFrame,
                               rolling_window: Union[int, str],
                               rolling_min_periods: int,
                               metric: str
        ) -> pd.DataFrame:
        """
            Calculates the rolling sum of precipitation over a specified window.

            Args:
                data (pd.DataFrame): Input DataFrame containing 'lat', 'lon', 'Date', and 'precip' columns.
                rolling_window (Union[int, str]): Rolling window size (e.g., '45D' for 45 days or an integer for rows).
                rolling_min_periods (int): Minimum number of observations required to compute the rolling sum.
                metric (str): Column name to assign to the computed rolling sum.

            Returns:
                pd.DataFrame: DataFrame containing 'lat', 'lon', 'Date', and the computed rolling sum.
            """
        if not {'lat', 'lon', 'Date', 'value'}.issubset(data.columns):
            raise ValueError("Input DataFrame must contain 'lat', 'lon', 'Date', and 'precip' columns.")

        # Ensure 'Date' is in datetime format
        data['Date'] = pd.to_datetime(data['Date'])

        # Sort data before applying rolling calculations
        data = data.sort_values(['lat', 'lon', 'Date']).reset_index(drop=True)

        # Perform rolling sum calculation
        rolling_sum = (
            data.set_index('Date')
            .sort_index()
            .groupby(['lat', 'lon'])['value']
            .rolling(window=rolling_window, min_periods=rolling_min_periods)
            .sum()
            .reset_index()
            .rename(columns={'value': metric})
        )

        return rolling_sum


    def getWeatherRegion(self, country: str, state: str)-> List[float]:
        """
            Retrieves the polygon coordinates for a specific location and sub-location
            from a JSON file.

            Args:
                location (str): The main location (e.g., country).
                sub_location (str): The sub-location (e.g., state or region).

            Returns:
                List[float]: A list of coordinates representing the polygon of the region.
                              If no match is found, returns an empty list.

            Raises:
                FileNotFoundError: If the JSON file does not exist for the given location.
                ValueError: If the JSON data cannot be decoded correctly.
                RuntimeError: For any other errors during file processing or data extraction.
            """
        # Construct file path
        file_name = os.path.join(WORKING_DIR, 'Regions', f'crop_regions_{self.country}.json')

        # Check if the file exists
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"File not found: {file_name}")

        try:
            # Open and load the JSON data from the file
            with open(file_name, 'r') as json_file:
                loaded_data = json.load(json_file)

            # Search for the matching country and state
            for data in loaded_data:
                if data.get('location') == self.country and data.get('sub_location') == self.state:
                    return data.get('coordinates', [])

        except json.JSONDecodeError:
            raise ValueError(f"Error decoding JSON in file: {file_name}")
        except Exception as e:
            raise RuntimeError(f"An error occurred while processing the file: {file_name} - {str(e)}")

        # Return an empty list if no matching data is found
        return []

    def get_coordinates_within_region(self, polygon: List[List[float]])-> gpd.GeoDataFrame:
        """
        Generates a grid of latitude and longitude points within the bounding box
        of a given polygon and filters the points that lie inside the polygon.

        Args:
            polygon (List[List[float]]): A list of [latitude, longitude] coordinates
                                         defining the polygon.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the latitude, longitude,
                              and geometry of points that lie within the polygon.
        """
        if not polygon:
            raise ValueError("Polygon coordinates cannot be empty.")

        # Determine the bounding box of the polygon
        min_lon = min([coordinate[1] for coordinate in polygon])
        max_lon = max([coordinate[1] for coordinate in polygon])
        min_lat = min([coordinate[0] for coordinate in polygon])
        max_lat = max([coordinate[0] for coordinate in polygon])

        # Generate latitude and longitude points within the bounding box
        lat = np.arange(int(min_lat * 4) / 4, int(max_lat * 4) / 4 + 0.25, 0.25)
        lon = np.arange(int(min_lon * 4) / 4, int(max_lon * 4) / 4 + 0.25, 0.25)

        # Create a grid of points (latitude, longitude) in the bounding box
        grid_points = np.array(np.meshgrid(lat, lon)).T.reshape(-1, 2)

        # Create a GeoDataFrame from the grid points and its geometry
        points_gdf = gpd.GeoDataFrame(pd.DataFrame({
            "lat": grid_points[:, 0],
            "lon": grid_points[:, 1]
        }),
            geometry=[Point(point) for point in grid_points])

        # Create a GeoSeries for the polygon

        polygon_gs = gpd.GeoSeries(geometry.Polygon(polygon))
        points_inside = points_gdf[points_gdf.within(polygon_gs.geometry.iloc[0])]

        return points_inside
