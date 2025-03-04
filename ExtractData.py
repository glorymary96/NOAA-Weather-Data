
import requests
from PARAMS import FILES_DIR

def get_data(metric, yr):
    # Define the years to fetch data for
    if metric in ['tmax', 'tmin']:
        years = [yr]
    elif metric in ['precip']:
        years = [yr, yr - 1]

    # Define a single output file to save the combined data
    output_file = f"{FILES_DIR}{metric}.nc"

    # Download and save the data for needed years
    with open(output_file, "wb") as outfile:
        for year in years:
            if metric in ['tmax', 'tmin']:
                file_url = f'https://downloads.psl.noaa.gov/Datasets/cpc_global_temp/{metric}.{year}.nc'
            elif metric in ['precip']:
                file_url = f'https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/{metric}.{year}.nc'

            file_response = requests.get(file_url)
            if file_response.status_code == 200:
                # Append the content to the output file
                outfile.write(file_response.content)
                print(f"Saved data for {metric} for year {year}")
            else:
                print(f"Failed to download data for {metric} for year {year}")
