## **Introduction**
It is so common to build etl script to make easy data pipeline for many goals in data science.
In this article, we interest in weather feature data on a website via its API. Along this article
We explain how to build efficiently an Extract, Transform, and Load pipeline and make yours easier with scheduling.
You need to know python programmation language, basic knowlegdes could be sufficient. So obviously you have to install Python 3.6 or later.

Are you ready ? Let’s get started !
We can understand airflow etl from installation to launching in 6 steps.

## **Step 1 : Create working virtual environment**
For this kind of project, it’s strongly recommand to create a virtual development environment to isolate your differents project dependencies needed and why not allow you reproduce easily that one for later whenever you want.
Let’s call this airflowenv, open your terminal and paste this one below :

```
python -m venv airflowenv
```

You might see a created new folder called airflowenv

## **Step 2 : Activate the virtual environment and Install airflow**

Generally it is easy to install airflow on Linux.

With my experience I have always encountered some error while installation so i recommand you installation on Linux. You don’t have Linux ? You can create an Linux image with Docker and Launch a container. Don’t worry it's easy to learn [Docker](https://depot.dev/blog/docker-build-image) using. 

-	Activate the environment For Unix-based systems
```
source airflowenv/bin/acitvate
```

-	install airflow
```
pip install apache-airflow
```


## **Step 3 : Create your python scripts**
Look ! We will extract, transform and load so we must create mainly three functions:
- **extract_data** : extract data from weather API website by requesting.
- **transform_data** : transformations depend on the goal you want to reach with data extracted. So here to be simply we will eventually keep only 3 features among all extracted for instance.
- **load_data** : save data to a specific folder.

You can write your own etl functions like this on contained in ``etl_functions.py`` module:

```
# Need for defining alias pd used by some functions describing entries type allowed
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
    print("Token d'API météo non trouvé. Assurez-vous de définir la variable d'environnement VISUALCROSSING_API_TOKEN.")
    sys.exit()
  else:
    pass
  
  # departements name processing
  pathtofile = "post1/centre_geographique-departement_fr.txt"
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
    filename = os.path.join("airflow/post1", key)
    dataframe.to_csv(filename, index=False)

```

Note : It might appear pyarrow librairy needed, if so install pyarrow. This error often appears when you handle pandas dataframe while serializing or deserializing.

```
pip install pyarrow==16.0.0
```


## **Step 4 : Create DAGs**
Create the pipeline dag file (Directly Acyclic Graph) and save it in the dags folder into airflow folder, you must create dags folder.

Here is your dags, let's call it ``france_data_pipeline_dag_test.py`` 

```
# track folder data containing our useful functions

import sys
sys.path.append("/data")

# import in the dag file any librairies needed for the project

import pandas as pd
import datetime
import urllib.request
import unidecode
from unidecode import unidecode
from datetime import datetime, timedelta
from etl_functions import *

from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'donatello',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 5, 8, 11, 58, 0),
}

# Create the DAG object
dag = DAG(
    'france_weather_data_pipeline',
    default_args=default_args,
    description='An end-to-end france weather data pipeline using Airflow and Python - test',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the extract_data task
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Define the transform_data task
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    op_args=[extract_task.output],
    provide_context=True,
    dag=dag,
)

# Define the save_to_local task
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    op_args=[transform_task.output],
    op_kwargs={'execution_date': '{{ ts }}'},
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task  >> load_task

```
It recommended to set an airflow environment variable, important whether need a token to extract datas from a website.
In this case hide your token in an environment variable for more security.

Set it like :

```
airflow variables set env_variable_name env_variable_value
```
It requires your ``env_variable_name`` started with ``AIRFLOW_VAR_`` and add its name after as ``**AIRFLOW_VAR_env_variable_name**``


## **Step 5 : Initialize db, create credentials (username email, and password), in short that is airflow user creating.**

You can find this step setting into the file ``entrypoint.sh``.

```
#!/usr/bin/env bash

# Initiliase the metastore
airflow db init

# Create user
# -u: --username; -p: --password; -r: --role; -e: --email; -f: --firstname; -l: --lastname
airflow users create \
        -u admin -p admin password \
        -r Admin \
        -e adminemail@mail.com \
        -f admin  \
        -l admin


# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in foreground (for logs)
airflow webserver --port 8080 

```


## **Step 6 Interact with your etl code**
Once server launched you might not use actual terminal window. You should open a new terminal and reactivate the airflow virtual environment you created.

- You have to move your dag file ``france_data_pipeline_dag_test.py`` to /ariflow/dags

Whether all is right you might see this page if you filter a specific dag:

![dag](/Assets/dag-2024-05-08-180848.png)

You can see also the matching dag graph 
![graph](/Assets/graph-2024-05-08-181037.png)

It is possible to interact with for specially debug if your code is still not ok .
![logs](/Assets/logs-2024-05-08-181145.png)


## **Summary**
I hope you enjoy reading this article about data pipeline creating with airflow.
We have learned how to set up step-by-step a airfow etl simply.
For render available permanently you may run your etl app onto cloud like AWS with EC2 for computing and S3 for data storage. If you have any question, write to me and i will give you an answer as soon as possible. For going further, pay attention to my next article. See you soon !

[Post 2 : How to deploy your airflow etl project onto cloud using AWS](_posts/2024-05-16-Deployment-Airflow-Data-Pipeline-on-AWS-Cloud.md)
