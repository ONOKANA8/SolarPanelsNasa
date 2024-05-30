

# import
import pandas as pd

# transformation of france input data from france datas extracted (done after extraction)
def transform_data_france(dataframe: pd.DataFrame):
    """Transform data from france data already extracted
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

    # replace name Saint-Martin-du-Mont because there are Saint-Martin-du-Mont(Ain) and Saint-Martin-du-Mont(Côte d'azur)
    dataframe.iloc[0, 1] = "Saint-Martin-du-Mont(Ain)"
    dataframe.iloc[21, 1] = "Saint-Martin-du-Mont(Cote d'Or)"
    
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
    
    dataframe.regions = new_list

    # create diff_date
    dataframe['datetime'] = pd.to_datetime(dataframe['datetime'])
    dataframe['sunset'] = pd.to_datetime(dataframe['sunset'])
    dataframe['sunrise'] = pd.to_datetime(dataframe['sunrise'])
    sunset_index = list(dataframe.columns).index("sunset")
    diff_value = (dataframe['sunset'] - dataframe['sunrise']) / pd.Timedelta(hours=1) #type: ignore
    dataframe.insert(sunset_index+1, "timeofday", round(diff_value, 1))
    
    return dataframe

# I decide to set accent of df1.dep word. otherwise we could withdraw accent of that is more simple,
# but I keep the first idea because it is so pretty to have accent during analyzing and visualizing

def set_dep_accent(list_dep):
    """
    Only for Bouches-du-Rhone, Ariege, Cotes-d'Armor, Deux-Sevres, Correze, Isere
    Cote-d'Or, Pyrenees-Orientales, Saone-et-Loire, Finistere, Hautes-Pyrenees, Lozere
    Puy-de-Dome, Ardeche, Rhone, Vendee, Haute-Saone, Herault, Pyrenees-Atlantiques, Nievre
    Drome"""

    new_list = []
    for i in list_dep:
        if i=="Bouches-du-Rhone":
           new_list.append("Bouches-du-Rhône")
        elif i=="Ardeche":
            new_list.append("Ardèche")
        elif i=="Drome":
            new_list.append("Drôme")
        elif i=="Hautes-Pyrenees":
            new_list.append("Hautes-Pyrénées")
        elif i=="Nievre":
            new_list.append("Nièvre")
        elif i=="Herault":
            new_list.append("Hérault")
        elif i=="Ariege":
            new_list.append("Ariège")
        elif i=="Correze":
            new_list.append("Corrèze")
        elif i=="Rhone":
            new_list.append("Rhône")
        elif i=="Isere":
            new_list.append("Isère")
        elif i=="Cote-d'Or":
            new_list.append("Côte-d'Or")
        elif i=="Pyrenees-Atlantiques":
            new_list.append("Pyrénées-Atlantiques")      
        elif i=="Deux-Sevres":
            new_list.append("Deux-Sèvres")    
        elif i=="Cotes-d'Armor":
            new_list.append("Côtes-d'Armor")    
        elif i=="Puy-de-Dome":
            new_list.append("Puy-de-Dôme")   
        elif i=="Saone-et-Loire":
            new_list.append("Saône-et-Loire")
        elif i=="Finistere":
            new_list.append("Finistère")    
        elif i=="Vendee":
            new_list.append("Vendée")  
        elif i=='Haute-Saone':
            new_list.append("Haute-Saône")
        elif i=="Lozere":
            new_list.append("Lozère")
        elif i=="Pyrenees-Orientales":
            new_list.append("Pyrénées-Orientales")
        else:
            new_list.append(i)
                    
    return new_list


def transform_string(unicode_string: str):
    """transform words with accent to unaccented words
    """
    import unicodedata
    import re
    
    # Normaliser la chaîne Unicode pour convertir les caractères accentués en caractères de base
    normalized_string = unicodedata.normalize('NFKD', unicode_string)

    # Remplacer directement les caractères accentués par leurs équivalents non accentués à l'aide d'une expression régulière
    return re.sub(r'[^\x00-\x7F]', '', normalized_string)

def retrieve_json_dep_data():
    """retrieve departments data
    """
    import pandas as pd
    df_departments = pd.read_json("datas/cities_center_of_france_departements_data.json")
    df_departments = df_departments.T
    df_departments.insert(0, "department_code", df_departments.index)
    df_departments.iloc[51, 2] = "Saint-Pierre"
    return df_departments
    

### **Create data for Analytics**
def data_for_data_mart():
    """have all datas we need for specifical analysis
    """
    from days_filter import concat_seven_last_days_function
    
    dataframe = concat_seven_last_days_function()
    dataframe = transform_data_france(dataframe)
    dataframe = dataframe[["communes", "regions", 'temp', 'solarradiation', 'solarenergy', 'uvindex',
                            'timeofday', 'windgust', 'windspeed', 'winddir', 'cloudcover', 'precip', 'visibility', "datetime"]]
    dataframe.communes = dataframe.communes.apply(transform_string)
    df_departments = retrieve_json_dep_data()
    dataframe = dataframe.merge(df_departments, how ='inner', left_on='communes', right_on='centre')
    solarenergy_index = list(dataframe.columns).index("solarenergy")
    dataframe.insert(solarenergy_index+1, "solarenergy_kwh", round(dataframe.solarenergy*0.2778, 1))


    def convert_temp_tocelsius(fahrenheit):
        """
        args : temp in °F 
        return : temp in °C, celsius = (fahrenheit - 32) * 5/9
        """
        return round((fahrenheit - 32)*(5/9))

    # convert fahrenheit to celsius 
    dataframe.temp = dataframe.temp.apply(convert_temp_tocelsius)
    # replace some data dep values
    dataframe.dep = set_dep_accent(dataframe.dep)

    dataframe = dataframe[["regions", "dep", 'solarradiation', "temp", 'solarenergy', 'solarenergy_kwh', 'uvindex', 'timeofday', 
                            'windgust', 'windspeed', 'winddir', 'cloudcover', 'precip', 'visibility', "datetime"]]
    
    return dataframe


### **Create data for Analytics**
def test_data_for_data_mart():
    """have all datas we need for specifical analysis
    """
    from queries_function import concat_function
    
    dataframe = concat_function()
    dataframe = transform_data_france(dataframe)
    dataframe = dataframe[["communes", "regions", 'temp', 'solarradiation', 'solarenergy', 'uvindex',
                            'timeofday', 'windgust', 'windspeed', 'winddir', 'cloudcover', 'precip', 'visibility', "datetime"]]
    dataframe.communes = dataframe.communes.apply(transform_string)
    df_departments = retrieve_json_dep_data()
    dataframe = dataframe.merge(df_departments, how ='inner', left_on='communes', right_on='centre')
    solarenergy_index = list(dataframe.columns).index("solarenergy")
    dataframe.insert(solarenergy_index+1, "solarenergy_kwh", round(dataframe.solarenergy*0.2778, 1))


    def convert_temp_tocelsius(fahrenheit):
        """
        args : temp in °F 
        return : temp in °C, celsius = (fahrenheit - 32) * 5/9
        """
        return round((fahrenheit - 32)*(5/9))

    # convert fahrenheit to celsius 
    dataframe.temp = dataframe.temp.apply(convert_temp_tocelsius)
    # replace some data dep values
    dataframe.dep = set_dep_accent(dataframe.dep)

    dataframe = dataframe[["regions", "dep", 'solarradiation', "temp", 'solarenergy', 'solarenergy_kwh', 'uvindex', 'timeofday', 
                            'windgust', 'windspeed', 'winddir', 'cloudcover', 'precip', 'visibility', "datetime"]]
    
    return dataframe
### **Create Data Mart for Analytic**
# Extract string to list function 
def extract_string_from_list(string_list):
    """function to transform extract data from a string of list
    Args : "['Lot-et-Garonne']"
    return : 'Lot-et-Garonne'
    """
    import ast
    return ast.literal_eval(string_list)[0]


# departments from shapefile 
def retrieve_departments_from_shapefile():
    """
    Retrieve file contained departments geo data (polygon) 
    """
    import geopandas as gpd # type: ignore
    geodataframe = gpd.read_file("georef-france-departement/georef-france-departement-millesime.shp", encode='utf-8')
    # choose columns we interst in 
    geodataframe = geodataframe.query("dep_area_co=='FXX'")[["dep_name", "geometry"]]
    # transform reg_name
    geodataframe.dep_name = geodataframe.dep_name.apply(extract_string_from_list)
    return geodataframe


# regions from shapefile
def retrieve_regions_from_shapefile():
    """
    Retrieve file contained regions geo data (polygon)

    """
    import geopandas as gpd # type: ignore
    geodataframe = gpd.read_file("georef-france-region-millesime@public/georef-france-region-millesime.shp", encode='utf-8')
    # choose columns we interst in 
    geodataframe = geodataframe.query("year=='2023' and reg_area_co=='FXX'")[["reg_name", "geometry"]]
    # transform reg_name
    geodataframe.reg_name = geodataframe.reg_name.apply(extract_string_from_list)
    return geodataframe


def departments_data_mart():
    """this function creates final data for departments analytic
        it uses data from data filter during concat_seven_last_days_function function
    """
    import geopandas as gpd # type: ignore

    # choropleth data for departments
    departments = data_for_data_mart()

    # departments from shapefile
    geodataframe = retrieve_departments_from_shapefile()
    # aggregate numeric columns by regions object : Group the DataFrame by regions
    dep_groupby_object = departments.groupby(by="dep")[["temp", 'solarradiation', 'solarenergy',
       'solarenergy_kwh', 'uvindex', 'timeofday', 'windgust', 'windspeed', 'cloudcover',
       'winddir', 'precip', 'visibility']]

    # Create Groupby object statistic paramete
    geo_departments = dep_groupby_object.mean().round(1)
    geo_departments = geo_departments.reset_index()

    # Add Legend
    geo_departments['solarenergy_kwh_Legend'] = [f"{ligne.dep}: {ligne.solarenergy_kwh} kWh/m²" \
                                    for _, ligne in geo_departments.iterrows()]

    # we can define others Legends as much as possible
    # ...
    geo_departments = geo_departments.merge(geodataframe, how='inner', left_on="dep", right_on="dep_name")
    geo_departments = gpd.GeoDataFrame(geo_departments)
    return geo_departments

def test_departments_data_mart():
    """this function creates final data for departments analytic
        it uses data from data filter during concat_seven_last_days_function function
    """
    import geopandas as gpd # type: ignore

    # choropleth data for departments
    departments = test_data_for_data_mart()

    # departments from shapefile
    geodataframe = retrieve_departments_from_shapefile()
    # aggregate numeric columns by regions object : Group the DataFrame by regions
    dep_groupby_object = departments.groupby(by="dep")[["temp", 'solarradiation', 'solarenergy',
       'solarenergy_kwh', 'uvindex', 'timeofday', 'windgust', 'windspeed', 'cloudcover',
       'winddir', 'precip', 'visibility']]

    # Create Groupby object statistic paramete
    geo_departments = dep_groupby_object.mean().round(1)
    geo_departments = geo_departments.reset_index()

    # Add Legend
    geo_departments['solarenergy_kwh_Legend'] = [f"{ligne.dep}: {ligne.solarenergy_kwh} kWh/m²" \
                                    for _, ligne in geo_departments.iterrows()]

    # we can define others Legends as much as possible
    # ...
    geo_departments = geo_departments.merge(geodataframe, how='inner', left_on="dep", right_on="dep_name")
    geo_departments = gpd.GeoDataFrame(geo_departments)
    return geo_departments

def regions_data_mart():
    """This function creates final data for regions analytic
       It uses data from data filter during concat_seven_last_days_function function
    """
    import geopandas as gpd # type: ignore

    # choropleth data for departments
    regions = data_for_data_mart()
    # regions from shapefile file
    geodataframe = retrieve_regions_from_shapefile()
    # aggregate numeric columns by regions object : Group the DataFrame by regions
    reg_groupby_object = regions.groupby(by="regions")[['temp', 'solarradiation', 'solarenergy',
       'solarenergy_kwh', 'uvindex', 'timeofday', 'windgust', 'windspeed', 'cloudcover',
       'winddir', 'precip', 'visibility']]

    # Create Groupby object statistic paramete
    geo_regions = reg_groupby_object.mean().round(1)
    geo_regions = geo_regions.reset_index()

    # Add Legend
    geo_regions['solarenergy_kwh_Legend'] = [f"{ligne.regions}: {ligne.solarenergy_kwh} kWh/m²" \
                                    for _, ligne in geo_regions.iterrows()]
    # we can define others Legends as much as possible
    # ...
    
    # choropleth data for regions analysis
    geo_regions = geo_regions.merge(geodataframe, how='inner', left_on="regions", right_on="reg_name")
    geo_regions = gpd.GeoDataFrame(geo_regions)
    return geo_regions

def test_regions_data_mart():
    """This function creates final data for regions analytic
       It uses data from data filter during concat_seven_last_days_function function
    """
    import geopandas as gpd # type: ignore

    # choropleth data for departments
    regions = test_data_for_data_mart()
    # regions from shapefile file
    geodataframe = retrieve_regions_from_shapefile()
    # aggregate numeric columns by regions object : Group the DataFrame by regions
    reg_groupby_object = regions.groupby(by="regions")[['temp', 'solarradiation', 'solarenergy',
       'solarenergy_kwh', 'uvindex', 'timeofday', 'windgust', 'windspeed', 'cloudcover',
       'winddir', 'precip', 'visibility']]

    # Create Groupby object statistic paramete
    geo_regions = reg_groupby_object.mean().round(1)
    geo_regions = geo_regions.reset_index()

    # Add Legend
    geo_regions['solarenergy_kwh_Legend'] = [f"{ligne.regions}: {ligne.solarenergy_kwh} kWh/m²" \
                                    for _, ligne in geo_regions.iterrows()]
    # we can define others Legends as much as possible
    # ...
    
    # choropleth data for regions analysis
    geo_regions = geo_regions.merge(geodataframe, how='inner', left_on="regions", right_on="reg_name")
    geo_regions = gpd.GeoDataFrame(geo_regions)
    return geo_regions