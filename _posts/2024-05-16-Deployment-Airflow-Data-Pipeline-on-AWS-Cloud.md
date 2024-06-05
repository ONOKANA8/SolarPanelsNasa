# **Introduction**
This post is a kind of rest of post 1. We show here how to render available anytime our airflow ETL dag. AWS cloud is technology used to reach our goal. Basically for this project we will use S3(Simple Storage Service), EC2 instance(Elastic Cloud Computing) and IAM(Identity and Access Management). 

![image-s3-iam-ec2-airflow](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/infrastructures-organization.drawio.jpg?raw=true)

Here is how this infrastructure has been set up: 

# **1. Create cloud infrastructures : EC2 and S3**

## **1.1 Create S3 buckets**

1 - Access to [AWS Management console](https://aws.amazon.com/console/)

2 - Select S3 service at the left top of your page

3 - Click on "Create Bucket" button

4 - Follow settings up configurations (name of your buckets, region, security options according to your needs)

I let you look at this [youtube video](https://www.youtube.com/watch?v=i4YFFWcyeFM). Perhaps it can ease your understanding.

For this project, we need to create a bucket named ``bucket-airflowpipeline-solarpanel-france``.

Finally we have had our bucket like this:
![bucket page](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/Bucket-page.png?raw=true)


## **1.2 Create EC2 instance**
1 - Access to [AWS Management console](https://aws.amazon.com/console/)

2 - Select EC2 service at the left top of your page

3 - Pick up proper AMI (Amazon Machine Image) by beginning to launch an instance and you might fall on this page:
Set up option you need:
![AMI chosing page](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/AMI-chosing-page.png?raw=true)

4 - Create key pair and download the private key (ssh-key-ed25519.pem for example):
![required-pair-key-and-settings.png?raw=true](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/required-pair-key-and-settings.png?raw=true)

As for me, I have chosen Ubuntu Server 24.04 LTS and t2.micro type of instance, first of all it is free tier eligible and extras hours are less expensive than others instances types, That is cool! Isn't that?:
![My AMI](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/my-ami.png?raw=true)

## **1.3 Now we need to connect S3 and EC2**
1 - Access to [AWS Management console](https://aws.amazon.com/console/)

2 - Select IAM(Identy Access Management) service

3 - Create IAM policy which will allow connexion to S3 bucket

4 - Attach this policy to an IAM role then Join it to your EC2 instance.

Here are some effortless steps:

**- Step 1** : Fortunately some existing policies already defined to ease this steps according to your need. For instance in this project we need a full EC2 access to S3 (each other access): we can find AWS managed policies we want already defined:
![policies-already-defined](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/aws-managed-policies.png?raw=true)

You will able to see ``AmazonEC2FullAccess`` and ``AmazonS3FullAccess`` that we will attach to a IAM role.

**- Step 2** : Once we create our own policies or identify aws managed policies you need, we must attach them to a role. For this project we will name it ``ec2-S3-airflow-solarpanel-role``:

Here is the role created:
![our-role-page](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/our-role-page.png?raw=true)

Then, How to create it:
- Click on ``Create role`` which at right at the top of page.
  
- Choose AWS service and select service you need, as it happens EC2:
![create-role-page](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/create-role-page.png?raw=true)

- Attach permissions policies you need, as it happens ``AmazonEC2FullAccess`` and ``AmazonS3FullAccess``:
![add-permission-page](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/attach-permission-page.png?raw=true)

- Give a meaningful name of your role to identify easily this role:
![name-role](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/name-role.png?raw=true) 

After all of that, we can see  ``ec2-S3-airflow-solarpanel-role`` created:
![ec2-S3-airflow-solarpanel-role](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/our-created-role.png?raw=true)

- It remains to attach it to EC2 instance like that:
 Open EC2 console, in the instance select ec2 instance created, click on ``Actions`` and pick up ``Modify IAM role`` in ``Security`` options, then select ``ec2-S3-airflow-solarpanel-role`` and click on ``Update IAM role``.

 ![role-attached-to-ec2](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/update-role.png?raw=true)

Now, your instance has full access to your Amazon S3 buckets. Your EC2 instance will be able to interact (write, read,...).
You will not need to create access key to connect with.

So far so good!

# **2. How to connect to EC2 instance** 

We decide to pilote operation from local machine. In this case we need to connect our machine (client) with remote instance EC2. Do you Remind We created SSH(Secure SHell) during cloud computing setting up? Well we need it to create this connexion.
I work onto a Windows system so i see at first PuTTY as software. PuTTY is a terminal emulator for Windows allowing connection to a remote machine via SSH protocol. Personally I have encountered a issue with PuTTY on this project, finally all goes well :smile:. I needed opening several terminals, 2 to be precise, you will understand why. 

Putty interface you need is like this one below:

![putty inetrface](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/putty_interface.png?raw=true)

You may enter the public ip of your EC2 instance on ``IP address``. After that you must browse to your ssh key path:
Go to ``SSH > Auth > credentials`` then enter the path and click on ``Open`` button. 

![putty inetrface](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/putty_interface.png?raw=true)

You might be invited to type username of your instance.
If all goes well you might fall on your ec2 terminal like this :

![ec2_terminal_interface](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/ec2_terminal_interface.png?raw=true)

From local Linux Virtual Machine, be sure you have OpenSSH client, if not, firstly install it :

```
sudo apt-get install openssh-client
```

and type this to connect to EC2 instance:
```
ssh -i path/to/your/ssh-key ec2username@Ip-address
```


# **3. Scripts**

We modify Scripts defined in [Post 1](https://github.com/ONOKANA8/SolarPanelsNasa/edit/airflowetl/_posts/2024-05-08-Weather-data-ETL-using-airflow-and-python-scripts.md?raw=true).
Here are them:
- dag .py : france_data_pipeline_dag.py
- our functions defined in a module : france_etl_functions.py

We store them inside a folder named data_france as well as the text file ``centre_geographique-departement_fr.txt``. This one contains name of towns of France making up as well as possible the center of departement they belong. This approach could allow us to estimate the average of features we could interest in from my point of view.
Here are scripts:

**france_etl_functions.py**:

```

# because of using it to define pandas dataframe arguments of some functions
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
  pathtofile = "~/solarpanel-data-extraction/data_france/centre_geographique-departement_fr.txt"
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
def transform_data(dataframe: pd.DataFrame):
    """
    args: france dataframe
    return: dataframe transformed
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
            regions.append(i[1])
            pays.append('')
        else:
            pass
    dataframe.insert(1, "communes", communes)
    dataframe.insert(2, "regions", regions)
    dataframe.insert(3, "pays", pays)

    # replace some wrong behaviors when defining regions
    new_list = []
    for i in list(dataframe.regions):
        if i=="Les Avanchers-Valmorel":
            new_list.append("Auvergne-Rhône-Alpes")
        elif i=="Essarts en Bocage":
            new_list.append("Pays de la Loire")
        elif i=="La Chapelle-Anthenaise":
            new_list.append("Pays de la Loire")
        elif i=="Wakiso":
            new_list.append("Occitanie")
        elif i=="San Benedetto Po":
            new_list.append("Île-de-France")
        elif i=="Vorarlberg":
            new_list.append("Grand Est")
        elif i=="Sanilhac":
            new_list.append("Auvergne-Rhône-Alpes")
        elif i=="Saint Pierre and Miquelon":
            new_list.append("Provence-Alpes-Côte d'Azur")
        else:
            new_list.append(i)

    # define new values for regions
    dataframe.regions = new_list

    # create diff_date
    dataframe['datetime'] = pd.to_datetime(dataframe['datetime'])
    dataframe['sunset'] = pd.to_datetime(dataframe['sunset'])
    dataframe['sunrise'] = pd.to_datetime(dataframe['sunrise'])
    sunset_index = list(dataframe.columns).index("sunset")
    diff_value = (dataframe['sunset'] - dataframe['sunrise']) / pd.Timedelta(hours=1)
    dataframe.insert(sunset_index+1, "timeofday", round(diff_value, 1))

    return dataframe

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
    import boto3
    s3 = boto3.client("s3")
    bucket_name = "bucket-airflowpipeline-solarpanel-france"
    key = f"france_data_{timestamp}.csv"
    csv_data = dataframe.to_csv(index=False)
    s3.put_object(
             Bucket=bucket_name,
             Key=key,
             Body=csv_data.encode("utf-8")
    )

```

**france_data_pipeline_dag.py**:
  
```

# track folder data containing our useful functions
import sys
sys.path.append("~/solarpanel-data-extraction/data_france")

# import in the dag file any librairies needed for the project
import pandas as pd
import datetime
import urllib.request
import unidecode
from unidecode import unidecode
from datetime import datetime, timedelta
from france_etl_functions import *

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
    'start_date': datetime(2024, 5, 16, 23, 0, 0),
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

We can see we track our folder ``data_france`` with library ``sys`` on dag .py script:

```
import sys
sys.path.append("~/solarpanel-data-extraction/data_france")
```

It is necessary to set that to be able to import functions from france_etl_functions.py inside ``data_france`` folder so that Python interpreter will track it permanently.


# **4. Airflow setting up**

At first on the terminal create a virtual environment : we name it airflowenv

```
python3 -m venv airflowenv
```

Activate it :

```
source airflowenv/bin/activate
```

You will see:

![airflowenv](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/activate-airflowenv.png?raw=true)

Secondly install airflow :
```
pip install airflow==2.9.1
```

# **5. Create database, airflow user and launch webserver and scheduler**

Here we need to open another terminal window before launching the script below.

```
#!/usr/bin/env bash

# Initiliase the metastore
airflow db init

# Create user
airflow users create \
        -u username -p password \
        -r Admin \
        -e user@mail.com \
        -f firstname  \
        -l lastname


# Run the scheduler in background
airflow scheduler &> /dev/null &

# Run the web server in foreground (for docker logs)
airflow webserver --port 8080
```

After that webserver will monopolize the terminal and so we will not able to use it to interact with ec2: the second opened will allow us to access ec2 instance.


# **6. Monotoring of your Airflow DAG**

Now it is time to move or copy your dag file in a special folder inside airflow folder: ``dags`` folder, not another name, just ``dags``. At first you have to create ``dags`` and move your dag file into.
If all goes well you might see the webpage below after typing in a browser ``ec2-ip-address:8080``: 

![webpage with france_dag](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/displaying-of-your-dag.png?raw=true).

Wait! a another stuff is required: you have to set the ``secure group rules`` with a protocol which listen the port 8080. Pay well attention to the inbound and outbound rules you define, it is very crucial for the traffic allowance!  

Now you can see your dag and monotor as you want according to issues you could encounter and debug in real time on your terminal. As I have told in the past post, you can see on airflow website your dag code and also logs when something is wrong on your codes.  

Here is the csv file we intended to load. We can see it in the bucket ``bucket-airflowpipeline-solarpanel-france`` on AWS S3, we will have time to analyze datas it contains and create insights with:
![first csv file intended](https://github.com/ONOKANA8/SolarPanelsNasa/blob/airflowetl/images/filesstoreins3.png?raw=true).


# **Summary**

Now you know how to deploy your airflow ETL dag on AWS Cloud with its services like EC2, S3, IAM and so make your dag available anytime you want. It is more practical not only because of availability but also the capabilities of scaling resources you use by the amounts of tasks you achieve.
I hope you have understood everything I explain above and if you have any question, write to me and i will give you an answer as soon as possible. 

See you soon !


[Post 3 : Analyzing Datas extracted handling with Power BI(Writing ongoing)]()
