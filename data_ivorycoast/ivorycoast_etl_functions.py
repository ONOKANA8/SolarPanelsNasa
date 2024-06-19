# need for 
import pandas as pd

# Our etl functions will operate with this three below what I call subfunctions:
def read_names_from_txt(filename):
    """Reads names from a text file and stores them in a list.
    Args:
        filename (str): The path to the text file containing the names.
    Returns:
        list: A list of names extracted from the text file.
    """
    with open(filename, 'r') as f:
        names = []
        for line in f:
            name = line.strip()  # Remove leading/trailing whitespace
            if name:  # Ignore empty lines
                names.append(name)
    return names


def string_accent_less(enter):
  """
  Args : take any string
  Return : practical accent_less string
  """
  from unidecode import unidecode # type: ignore
  y = list(map(lambda x: unidecode(x), enter))
  return y


# keep cities only in the first list
def get_unique_elements(list_a: list) -> list:
    """Finds elements that are unique to list_a (present in list_a but not in list_b).
    Args:
        list_a: The first input list.
        list_b: The second input list.
    Returns:
        A list containing elements unique to list_a.
    """

    unique_elements = []
    list_b = ['Koro', 'Tai', 'Oume', 'Divo', 'Buyo', 'Kong', 'Man', 'Kani']
    set_a = set(list_a)
    set_b = set(list_b)

    for element in set_a:
        if element not in set_b:
            unique_elements.append(element)

    return unique_elements



#### Now Our three functions ####

# for extracting datas
def extract_data():
  """
    Extraction de données climatique via le site de visualcrossing
     Intégration deslibrairies requises pour des requetes parallèles
     pathtofile : the path using to access text file containing departements you want: you can add in others cities name
  """
  import pandas as pd
  import urllib.request
  import sys
  import csv
  import codecs
  import ssl
  import os
  from airflow.models import Variable
  
  
  # load environment variable retrieve request token during that one launching
  visualcrossing_api_token = Variable.get("AIRFLOW_VAR_VISUALCROSSING_API_TOKEN")
  if not visualcrossing_api_token:
    print("Token d'API météo non trouvé. Assurez-vous de définir la variable d'environnement VISUALCROSSING_API_TOKEN.")
    sys.exit()
  else:
    pass
  
  # departements name processing
  pathtofile = "/home/ubuntu/SolarPanelsNasa/data_ivorycoast/departements_ci.txt"
  departements = read_names_from_txt(pathtofile)
  departements = string_accent_less(departements)
  departements = get_unique_elements(departements)
  
  # avoid authentication issues
  ssl._create_default_https_context = ssl._create_unverified_context
  
  dataframe_table = pd.DataFrame(data=[])

  for i, city in enumerate(departements):
    try: 
        ResultBytes = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}?unitGroup=us&include=days&key={visualcrossing_api_token}&contentType=csv")
        # Parse the results as CSV
        CSVText = csv.reader(codecs.iterdecode(ResultBytes, 'utf-8'))
        city_data = list(CSVText)

        if i==0:
           dataframe_table = pd.DataFrame(data=dataframe_table, columns=city_data[0])
           dataframe_table.loc[len(dataframe_table)] = city_data[1]
        else:
           dataframe_table.loc[len(dataframe_table)] = city_data[1]
           
    except urllib.error.HTTPError  as e: # type: ignore
        ErrorInfo = e.read().decode() 
        print('Error code: ', e.code, ErrorInfo)
    except  urllib.error.URLError as e: # type: ignore
        ErrorInfo = e.reason
        print('Error code: ', ErrorInfo)
  return dataframe_table
    

# for transforming datas
def transform_data(dataframe: pd.DataFrame):
    """
    Args: dataframe
    Return: dataframe transformed
    """
    import pandas as pd
    def split_name(columns):
        split_words = columns.split(", ", 2)
        return split_words

    split = dataframe.name.apply(split_name)

    communes = []
    regions = []
    pays = []
    for i in split:
        if len(i)==3:
            communes.append(i[0])
            regions.append(i[1])
            pays.append(i[2])
        elif len(i)==2:
            communes.append(i[0])
            regions.append('')
            pays.append(i[1])
        else:
            pass

    dataframe.insert(1, "communes", communes)
    dataframe.insert(2, "regions", regions)
    dataframe.insert(3, "pays", pays)

    # create diff_date
    dataframe['datetime'] = pd.to_datetime(dataframe['datetime'])
    dataframe['sunset'] = pd.to_datetime(dataframe['sunset'])
    dataframe['sunrise'] = pd.to_datetime(dataframe['sunrise'])
    sunset_index = list(dataframe.columns).index("sunset")
    diff_value = (dataframe['sunset'] - dataframe['sunrise']) / pd.Timedelta(hours=1) # type: ignore
    dataframe.insert(sunset_index+1, "timeofday", round(diff_value, 1))

    return dataframe


# for saving data to a folder defined before
def load_data(dataframe: pd.DataFrame):
    """
    Args: Pandas dataframe
    Save a pandas DataFrame to a csv file
    """
    from datetime import datetime
    execution_date = datetime.now()
    timestamp = execution_date.strftime("%Y%m%d-%H%M%S")

    # create an unique name with calling timestamp
    import boto3
    s3 = boto3.client("s3")
    bucket_name = "bucket-airflowpipeline-solarpanel-ivorycoast"
    key = f"france_data_{timestamp}.csv"
    csv_data = dataframe.to_csv(index=False)
    s3.put_object(
             Bucket=bucket_name,
             Key=key,
             Body=csv_data.encode("utf-8")
    )
