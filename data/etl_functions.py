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


# function for choosing cities, centre of departments of France and for further data extraction 
def get_final_cities_fr(first_cities_extract: list):
    """ 
    Args: first_cities_extract is list of france cities that extracted from file .txt
    Return: final list before extracting data with
    """
    for i, city in enumerate(first_cities_extract):
        if city=='Lano':
            first_cities_extract[i] = 'Erone'
        elif city=='Ahun':
            first_cities_extract[i] = "Moutier-d'Ahun"
        elif city=='La Marre':
            first_cities_extract[i] = 'Marre'
        elif city=='La Chapelle-Anthenaise':
            first_cities_extract[i] = 'Chapelle-Anthenaise'
        elif city=='Gurs':
            first_cities_extract[i] = 'Dognen'
        elif city=='Le Mans':
            first_cities_extract[i] = 'Coulaines'
        elif city=='Les Avanchers-Valmorel':
            first_cities_extract[i] = 'Avanchers-Valmorel'
        elif city=='Vicq':
            first_cities_extract[i] = 'Bardelle'
        elif city=='La Chaize-le-Vicomte':
            first_cities_extract[i] = 'Chaize-le-Vicomte'
        else:
            pass
    return first_cities_extract


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
    print("Weather API token not found. be sure define environnement variable VISUALCROSSING_API_TOKEN.")
    sys.exit()
  else:
    pass
  
  # departements name processing
  pathtofile = "/data/centre_geographique-departement_fr.txt"
  departements = read_names_from_txt(pathtofile)
  departements = string_accent_less(departements)
  departements = get_final_cities_fr(departements)
  
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
           
    except urllib.error.HTTPError  as e:
        ErrorInfo = e.read().decode() 
        print('Error code: ', e.code, ErrorInfo)
    except  urllib.error.URLError as e:
        ErrorInfo = e.reason
        print('Error code: ', ErrorInfo)
  return dataframe_table
    

# for transforming datas
def transform_data(dataframe_table: pd.DataFrame):
    dataframe_table = dataframe_table[["name", "solarenergy", "uvindex"]]
    return dataframe_table


# for saving data to a folder defined before
def load_data(dataframe: pd.DataFrame):
    """ 
    Args: Pandas dataframe
    Save a pandas DataFrame to a csv file
    """
    import os
    from datetime import datetime
    execution_date = datetime.now()
    timestamp = execution_date.strftime("%Y%m%d-%H%M%S")
    
    # create an unique name with calling timestamp
    key = f"france_data_{timestamp}.csv"
    filename = os.path.join("/data", key)
    dataframe.to_csv(filename, index=False)
