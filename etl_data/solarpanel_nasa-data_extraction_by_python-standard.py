"""
# **Get POWER Monthly and Annual API access on this route [✌](https://power.larc.nasa.gov/api/temporal/monthly/point)**
"""

import pandas as pd
import requests
import time
from etl_data.solarpanels_notebook_data_extraction import geolocat

Parameters = {
    "QV2M": "MERRA-2 Specific Humidity at 2 Meters (g/kg)",
    "RH2M": "MERRA-2 Relative Humidity at 2 Meters (%)",
    "ALLSKY_KT": "CERES SYN1deg All Sky Insolation Clearness Index (dimensionless)",
    "CLOUD_AMT": "CERES SYN1deg Cloud Amount (%)",
    "CLRSKY_KT": "CERES SYN1deg Clear Sky Insolation Clearness Index (dimensionless)",
    "TOA_SW_DWN": "CERES SYN1deg Top-Of-Atmosphere Shortwave Downward Irradiance (kW-hr/m^2/day)",
    "PRECTOTCORR": "MERRA-2 Precipitation Corrected (mm/day)",
    "ALLSKY_SFC_UVA": "CERES SYN1deg All Sky Surface UVA Irradiance (W/m^2)",
    "ALLSKY_SFC_UVB": "CERES SYN1deg All Sky Surface UVB Irradiance (W/m^2)",
    "ALLSKY_SRF_ALB": "CERES SYN1deg All Sky Surface Albedo (dimensionless)",
    "PRECTOTCORR_SUM": "MERRA-2 Precipitation Corrected Sum (mm)",
    "ALLSKY_SFC_SW_DNI": "CERES SYN1deg All Sky Surface Shortwave Downward Direct Normal Irradiance (kW-hr/m^2/day)",
    "ALLSKY_SFC_SW_DWN": "CERES SYN1deg All Sky Surface Shortwave Downward Irradiance (kW-hr/m^2/day)",
    "CLRSKY_SFC_SW_DWN": "CERES SYN1deg Clear Sky Surface Shortwave Downward Irradiance (kW-hr/m^2/day)",
    "ALLSKY_SFC_PAR_TOT": "CERES SYN1deg All Sky Surface PAR Total (W/m^2)",
    "ALLSKY_SFC_SW_DIFF": "CERES SYN1deg All Sky Surface Shortwave Diffuse Irradiance (kW-hr/m^2/day)",
    "CLRSKY_SFC_PAR_TOT": "CERES SYN1deg Clear Sky Surface PAR Total (W/m^2)",
    "ALLSKY_SFC_UV_INDEX": "CERES SYN1deg All Sky Surface UV Index (dimensionless)"
}

parameters = list(Parameters.keys())

power_url = "https://power.larc.nasa.gov/"
api_route = "api/temporal/monthly/point"
POWER_Monthly_Annual_API = power_url + api_route


def function_exec_time(funct):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = funct(*args, **kwargs)
        end = time.time()
        execution_time = end - start
        print(f"execution time of this function is : {(end - start):.4f} s")
        return result

    return wrapper


# latitude[i] , longitude[i]
params = {
    "start": 2013,
    "end": 2022,
    'latitude': 5.7362,
    "longitude": -5.3748,
    "community": "ag",
    "parameters": {"CLRSKY_SFC_PAR_TOT", 'ALLSKY_KT'},
    "format": "json",
    "user": None,
    "header": "False",
    "site-elevation": None,
    "wind-elevation": None,
    "wind-surface": None
}

"""Visualisation de la réponse de requête"""

city_table_response = requests.get(POWER_Monthly_Annual_API, params=params)
city_table_response = city_table_response.json()
city_table_response1 = city_table_response.get('properties')['parameter']['CLRSKY_SFC_PAR_TOT']

new_rows = pd.Series(city_table_response1.values())
new_rows = new_rows.to_frame().T


@function_exec_time
def insert_city_parameter(rows: pd.Series, ville: str, paramet: str):
    rows = rows.to_frame().T
    rows.insert(0, "ville", ville)
    rows.insert(0, "parameters", paramet)

    return rows


# constitution des colonnes du dataframe finale
col_name = list(city_table_response1.keys())
col_name.insert(0, "ville")
col_name.insert(0, "parameters")


# rows by a point(here a city) defined with latitude and longitude
# and any parameters it has
@function_exec_time
def POWER_Monthly_Annual_city_data(start: int, end: int, city: str, latitude: float, longitude: float, param: list,
                                   columns: list
                                   ):
    """
  give the parameters and parameters value of a city each month a year
  from start year to end year
  """

    sub_data_collect = pd.DataFrame(columns=columns)

    for i in param:
        params_ = {
            "start": start,
            "end": end,
            'latitude': latitude,
            "longitude": longitude,
            "community": "re",  # re as Renewable Energy,
            # sb as Sustainable Buildings
            # ag as AGroclimatology
            "parameters": i,
            "format": "json",
            "user": None,
            "header": "False",
            "site-elevation": None,
            "wind-elevation": None,
            "wind-surface": None}

        response = requests.get(POWER_Monthly_Annual_API, params=params_)
        response = response.json()
        response = response.get('properties')['parameter'][i]

        first_row = pd.Series(response.values())
        new_data = insert_city_parameter(first_row, city, i)
        new_data.columns = columns
        sub_data_collect = pd.concat([sub_data_collect, new_data],
                                     ignore_index=True)

    return sub_data_collect


new_rows_ = POWER_Monthly_Annual_city_data(
    start=2013,
    end=2022,
    city="Abidjan",
    latitude=5.320357,
    longitude=-4.016107,
    param=parameters,
    columns=col_name
)

data_stock = pd.read_csv("cities-data-and-location.csv")


@function_exec_time
def extractor_nasa_data(start: int, end: int):
    """
  this function needs to install geopy module in order to get location of each
  city

  even define columns name and cities
  """

    columns_name = list(city_table_response1.keys())
    columns_name.insert(0, "ville")
    columns_name.insert(0, "parameters")
    ci_cities = data_stock.ville
    data_collected = pd.DataFrame(columns=columns_name)

    for city in ci_cities:
        lat = geolocat.geocode(city).latitude  # remplacer par
        lon = geolocat.geocode(city).longitude

        new_row = POWER_Monthly_Annual_city_data(
            start=start,
            end=end,
            city=city,
            latitude=lat,
            longitude=lon,
            param=parameters,
            columns=columns_name
        )

        data_collected = pd.concat([data_collected, new_row],
                                   ignore_index=True)

    del lat, lon, new_row

    return data_collected


# udf_extractor_nasa_data = udf(extractor_nasa_data)
# nasa_re_data = udf_extractor_nasa_data(2013, 2022)

nasa_re_data = extractor_nasa_data(start=2013, end=2022)

"""This extractor takes too much more time to extract data. I think that time could be enhance with a best
and quick extractor. Its running time is over 6006 seconds.

"""

# Extracted data storage as **``csv``** file
nasa_re_data.to_csv("nasa-renewable-energy-data.csv", index=False)
