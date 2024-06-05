# **Introduction**

This post highlights the weather data retrieved in the previous post. We will seek to understand this data in order to extract informations by creating insights. This involves essentially descriptive analysis of our data. To achieve this, we will work with data processing and visualization tools such as Power Query, PowerBI, and Python.

![Powerbi and Python](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/python_powerbi-chart.jpg?raw=true)


# **1. Data Processing (to data_mart) with Power Query**

After extracting the data, it's clear that we want to analyze it. This involves weather data such as temperature, solar power, associated light energy, daylight duration, and many other important pieces of information. Our study here will focus on the energy data because, in my opinion, it is the primary and most important factor that gives us the light energy received by solar panels.

This involves a series of necessary preprocessing steps in Power Query: creating queries, merging queries, splitting columns to extract important information, changing column types, rearranging columns, renaming columns, deleting columns, replacing values, rounding numbers, and creating calculation functions. In short, there is a wide range of transformations possible with Power Query, making it a powerful data transformation tool.

![applied steps](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/applied-steps-window.png?raw=true)

All these steps lead us to a fact table that can be visualized both in Power Query and in Power BI.

![power query window](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/powerbi_table_visualization.png?raw=true)


# **2. Create dashboard**

## 2.1 **Create visual with Power BI**
This section allowed us to get a general overview of solar energy across France. We needed the departments associated with the communes where the weather data was recorded, so a table named "centres and departments" was created containing this information. In this context, Data Modeling is necessary to avoid less optimized joins for data analytics: the creation of relationships between tables. Here, this involves creating a many-to-one relationship between "centres and departments" and our fact table "france_data_by_week", primarily to retrieve the names of the departments to which the communes are attached. Additionally, this includes the creation of quick measures and dynamic columns (such as temperature in Celsius).

![data model](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/data-modeling.png?raw=true)


The Data Modeling step is meant to facilitate analysis in terms of processing speed and visualization. We primarily used visualization types such as **``maps, bar charts, choropleth maps, and line charts``**.

![charts](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/chart_types.png?raw=true)

With all this, we have finally obtained this dashboard using Power BI shown as below:

![weather dashboard](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/solar-project-dashboard.png?raw=true)

We can observe the evolution of sunlight over time, as well as the region or department that received the most sunlight and solar energy. It would also be interesting to create a choropleth map to provide a global view of France, highlighting the sunniest areas.


## **2.2 Create visuals with Power BI**
In this case, we will use ``Python in a notebook environment`` to observe the sunniest parts of France over the studied period. Here's a step-by-step approach to achieve this:

The steps leading to the desired visualization are:

1- Data retrieval

2- Data transformation to facilitate future joining

3- Retrieval of geolocation data (using the ``Geopandas`` library)

4- Merging value data with Geopandas data

5- Visualizing the data on a map using the choropleth function from the Folium module, which is well-known for creating maps with Python


Below the condensed code:

```

# imports
from days_filter import *
from data_mart import *
from reporting import *

# access departments data_mart for choropleth analytic
geo_departments = test_departments_data_mart()

# access departments data_mart for choropleth analytic
geo_regions = test_regions_data_mart()

# visualize regions choropeth values showing Average of sunshine
test_regions_sunshine_plot(geo_regions)
# visualize departments choropeth values showing Average of sunshine
test_departments_sunshine_plot(geo_departments)

```

Below is the choropleth map of the regions:

![regions choropleth](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/regions_choropleth.png?raw=true)

You can visualize the entire regions choropleth map [here](https://onokana8.github.io/SolarPanelsNasa/images/ensoleillement-regions.html). 
You can also find choropleth map of departments [there](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/images/ensoleillement-departements.html).

Note:

- You will find the created modules and their source code at the root of the project. These modules are ``data_mart.py, days_filter.py, queries_function.py, reporting.py``.
- A more suitable Python code could be used in Power BI (Python environment in Power BI, yes, it is possible to create visuals in Power BI today). We can detail this in another post.


# **3. What can we do with this information?**

It's evident that during this period, Corsica experienced more sunlight with an average of 6.3 kWh/m² per day, much to the delight of its residents. They can maximize this opportunity by installing photovoltaic panels on their south-facing roofs to achieve maximum efficiency. However, this study is quite limited, spanning only a week in terms of business potential.

Based solely on sunlight intensity, Corsica appears to offer the most profitability during this period for converting solar energy into electrical energy using photovoltaic panels. However, it's important to note that profitability depends on various factors. In reality, the power output of photovoltaic panels (through the photovoltaic cells) heavily relies on geographical location, weather conditions - as we've just seen - as well as roof orientation, tilt angle, or specific panel characteristics.

According to [HelloWatt](https://www.hellowatt.fr/panneaux-solaires-photovoltaiques/puissance-crete), an energy company, a residential photovoltaic installation project requires a power of 3 kWc. However, if you have very energy-intensive equipment, this power can reach 9 kWc (nominal power of the panel under optimal operating conditions).


Imagine you purchase a solar panel like the [Vertex type TSM-DE19R](https://static.trinasolar.com/sites/default/files/Datasheet_Vertex_DE19R_FR_2023%20C_web.pdf). It has an ``efficiency of 21.7%``. 

Below are the characteristic P-V curves of this panel:

![panel-features](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/features-p-v.png?raw=true)

It's understood that under optimal conditions, it can reach a peak power of 575 Wc for a solar irradiance of 1000 W/m² (direct solar radiation). In this case, it's possible to connect devices whose total power sums up to 575 W theoretically.

Below are the dimensions of our panel (**``2.7 m² surface area``**):

![panel dim](https://github.com/ONOKANA8/SolarPanelsNasa/blob/analysis/assets/panel-module-dim.png?raw=true)

With all the data, it's clear that if you live in Corsica with **``6.3 kWh/m² of energy``** during the studied week, and with an **``efficiency of 21.7%``** and a panel dimension of 2.7 m², the daily energy output would be calculated as follows:

**``6.3 kWh/m² * 2.7 m² * 21.7% = 3.7 kWh/day``**

So, the daily energy output would be ``approximately 3.7 kWh``.

Here is a summary table of what you can do daily with ``3.7 kWh``:

| **Utilization**         | **Daily Consumption (kWh)** | **Details of Daily Distribution**                             |
|----------------------|--------------------------|-----------------------------------------------------------|
| Lighting             | 0.5                      | Approximately 10 LED bulbs for 5 hours                    |
| Refrigerator         | 1.5                      | Throughout the day                                        |
| Television           | 0.5                      | 5 hours of operation                                      |
| Laptop               | 0.5                      | 10 hours of operation                                     |
| Washing Machine      | 1.0                      | 1 washing cycle                                           |
| Microwave            | 0.2                      | 12 minutes of operation                                   |


This table presents the daily consumption of energy in kWh for each utilization, along with details of the daily distribution.

Absolutely! With the collected data, it's entirely possible to plan the sizing of the solar panels according to your energy needs. Moreover, it could be extended to monthly planning. Gathering more data would indeed enhance precision in such planning endeavors.


# **Summary**
This use case really demonstrates the usefulness of the data obtained through ETL via the [VISUAL CROSSING API](https://www.visualcrossing.com/weather/weather-data-services), especially when combined with descriptive data visualizations for the specific period studied. It's worth noting that the data retrieved from the Visual Crossing website includes data for the current week (n) and up to two weeks ahead (n+14). This means that we can inform the public about energy forecasts by region or department for the next two weeks through data visualizations. This will be the subject of the next post, along with reporting visualizations in Power BI.

However, it's worth noting that a more comprehensive study would allow us to go further, potentially with complete data covering 1 to 2 years, and perhaps even the possibility of creating a time series model.

See you soon!
