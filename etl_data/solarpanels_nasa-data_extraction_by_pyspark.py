from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

import requests
import pandas as pd

power_url = "https://power.larc.nasa.gov/"
api_route = "api/temporal/monthly/point"
POWER_Monthly_Annual_API_URL = power_url + api_route

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

ville_position = pd.read_csv("cities-data-and-location.csv")
ville_position = ville_position[["ville", 'longitude', 'latitude']]


def fetch_data(climate_params):
    """
    Effectue une requête HTTP avec les paramètres donnés et retourne
    les données.
    """

    url = POWER_Monthly_Annual_API_URL
    response_json = requests.get(url, params=climate_params)
    response_json = response_json.json()
    return response_json.get('properties', {}).get('parameter', {}).get(climate_params["parameters"], {})


def process_partition(iterator):
    """
    Processus pour chaque partition du DataFrame.
    """
    data = []
    for climate_param in iterator:

        city_table_response = fetch_data(list(climate_param.values())[0])
        if city_table_response:
            new_row = (list(climate_param.values())[0]["parameters"],
                       list(climate_param.keys())[0], *city_table_response.values())

            data.append(new_row)
    return iter(data)


def POWER_Monthly_Annual_city_data(iterator: list, climate_param: list, columns: list):
    """
    Donner les paramètres et les valeurs des paramètres de chaque ville chaque
    mois d'une année de l'année de début à l'année de fin.
    """
    sparkobj = SparkSession.builder.appName("POWER_Monthly_Annual_city_data").getOrCreate()

    # Définir le schéma du DataFrame
    schema = StructType([
                            StructField("parameters", StringType(), True),
                            StructField("ville", StringType(), True)
                        ] + [StructField(col, FloatType(), True) for col in columns])

    # Créer un DataFrame vide
    sub_data_collect = sparkobj.createDataFrame([], schema)

    # Créer une RDD à partir des paramètres et des villes
    params_rdd = sparkobj.sparkContext.parallelize(iterator)

    # Appliquer la fonction de traitement à chaque partition de la RDD
    processed_rdd = params_rdd.mapPartitions(process_partition)

    # Convertir les données traitées en DataFrame Spark
    processed_df = processed_rdd.toDF(["parameters", "ville"] + columns)

    # Union avec le DataFrame vide
    sub_data_collect = sub_data_collect.union(processed_df)

    return sub_data_collect


# création de la lsite qui est l'itérateur every_params

spark = SparkSession.builder.appName("POWER_Monthly_Annual_city_data") \
    .getOrCreate()
spark_ville_position = spark.createDataFrame(ville_position)
spark_ville_position = spark_ville_position.collect()

every_params = []
for (i, record) in enumerate(spark_ville_position):
    ville, longitude, latitude = spark_ville_position[i]

    for param in parameters:
        params_position = {ville: {
            "start": 2013,
            "end": 2022,
            'latitude': latitude,
            "longitude": longitude,
            "community": "re",  # re as Renewable Energy,
            # sb as Sustainable Buildings
            # ag as AGroclimatology
            "parameters": param,
            "format": "json",
            "user": None,
            "header": "False",
            "site-elevation": None,
            "wind-elevation": None,
            "wind-surface": None
        }
        }

        every_params.append(params_position)

params = {
    "start": 2013,
    "end": 2022,
    'latitude': 5.320357,
    "longitude": -4.016107,
    "community": "re",  # re as Renewable Energy,
    # sb as Sustainable Buildings
    # ag as AGroclimatology
    "parameters": "QV2M",
    "format": "json",
    "user": None,
    "header": "False",
    "site-elevation": None,
    "wind-elevation": None,
    "wind-surface": None
}

response = requests.get(POWER_Monthly_Annual_API_URL, params=params)
response = response.json()
col_name = response.get('properties')['parameter'][params["parameters"]].keys()

city_data = every_params
parameters = parameters

result = POWER_Monthly_Annual_city_data(city_data, parameters, list(col_name))

# sauvegarde de la données
result.toPandas().to_csv("nasa-renewable-energy-data-pyspark.csv", index=False)
