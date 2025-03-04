#from WeatherRegions import WeatherRegions
from PARAMS import GET_COUNTRIES
from WeatherTranform import TransformWeatherRegions

regions = []

# Define month_start and month_end for each crop type
crop_seasons = {
    "HRW": (8, 7),
    "SRW": (8, 7),
    "HRS": (3, 10),
    "corn": (10, 9),
}

# Define regions for different countries as tuples
country_regions = {
    "US": {
        "HRW": [("Colorado", ""), ("Kansas", ""), ("Montana", ""), ("Nebraska", ""),
                ("Oklahoma", ""), ("South Dakota", ""), ("Texas", "")],
        "SRW": [("Alabama", ""), ("Arkansas", ""), ("Georgia", ""), ("Illinois", ""),
                ("Indiana", ""), ("Kentucky", ""), ("Michigan", ""), ("Mississippi", ""),
                ("Missouri", ""), ("North Carolina", ""), ("Ohio", ""), ("South Carolina", ""),
                ("Tennessee", ""), ("Virginia", "")],
        "HRS": [("Minnesota", ""), ("Montana", ""), ("North Dakota", ""), ("South Dakota", "")],
        "corn": [("Illinois", ""), ("Indiana", ""), ("Iowa", ""), ("Minnesota", ""),
                 ("Missouri", ""), ("Nebraska", ""), ("North Dakota", ""),
                 ("Ohio", ""), ("South Dakota", "")]
    }
}

# Populate regions list for selected countries
for country in GET_COUNTRIES:
    if country in country_regions:
        for crop_type, locations in country_regions[country].items():
            month_start, month_end = crop_seasons[crop_type]  # Retrieve predefined months
            for region, _ in locations:
                regions.append(TransformWeatherRegions(country, region, _, crop_type, month_start, month_end))
