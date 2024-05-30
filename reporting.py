def regions_sunshine_plot(geo_regions):
    """
    Display sunshine and solarenergy(kWh) of regions
    """

    import folium # type: ignore
    from datetime import datetime
    # Blank Map and create Map object via folium
  
    location=[47, 1]
    zoom_start = 5
    tiles = "cartodbpositron"

    Carte = folium.Map(location=location,
                    zoom_start=zoom_start,
                    tiles=tiles)

    geo_regions = geo_regions
    # Map visualization
    choropleth = folium.Choropleth(geo_data = geo_regions.to_json(),
                                    data = geo_regions,
                                    columns = ['reg_name', 'solarenergy_kwh'],
                                    key_on = 'feature.properties.reg_name',
                                    fill_color = 'OrRd',
                                    fill_opacity = 0.5,
                                    line_opacity = 1,
                                    legend_name = 'Ensoleillement (KWh/m².j)')

    # Add labels containing legend
    choropleth.geojson.add_child(
    folium.features.GeoJsonTooltip(['solarenergy_kwh_Legend'], labels=False) # type: ignore
    )

    # add choropleth
    choropleth.add_to(Carte)
    execution_date = datetime.now()
    timestamp = execution_date.strftime("%Y%m%d-%H%M%S")
    
    #Save carte and display
    Carte.save(f"assets/ensoleillement-regions{timestamp}.html")
    display(Carte) # type: ignore


def test_regions_sunshine_plot(geo_regions):
    """
    Display sunshine and solarenergy(kWh) of regions
    """

    import folium # type: ignore
    # Blank Map and create Map object via folium
    location=[47, 1]
    zoom_start = 5
    tiles = "cartodbpositron"

    Carte = folium.Map(location=location,
                    zoom_start=zoom_start,
                    tiles=tiles)

    geo_regions = geo_regions
    # Map visualization
    choropleth = folium.Choropleth(geo_data = geo_regions.to_json(),
                                    data = geo_regions,
                                    columns = ['reg_name', 'solarenergy_kwh'],
                                    key_on = 'feature.properties.reg_name',
                                    fill_color = 'OrRd',
                                    fill_opacity = 0.5,
                                    line_opacity = 1,
                                    legend_name = 'Ensoleillement (KWh/m².j)')

    # Add labels containing legend
    choropleth.geojson.add_child(
    folium.features.GeoJsonTooltip(['solarenergy_kwh_Legend'], labels=False) # type: ignore
    )

    # add choropleth
    choropleth.add_to(Carte)
    display(Carte) # type: ignore



def departments_sunshine_plot(geo_departments):
    """
    Display sunshine and solarenergy(kWh) of department
    """
    import folium # type: ignore
    from datetime import datetime
    # Blank Map and create Map object via folium
    location=[47, 1]
    zoom_start = 5
    tiles = "cartodbpositron"

    Carte = folium.Map(location=location,
                      zoom_start=zoom_start,
                      tiles=tiles)
    geo_departments = geo_departments
    # Map visualization
    choropleth = folium.Choropleth(geo_data = geo_departments.to_json(),
                                    data = geo_departments,
                                    columns = ['dep', 'solarenergy_kwh'],
                                    key_on = 'feature.properties.dep',
                                    fill_color = 'OrRd',
                                    fill_opacity = 0.5,
                                    line_opacity = 1,
                                    legend_name = 'Ensoleillement (KWh/m².j)')

    # Add labels containing legend
    choropleth.geojson.add_child(
      folium.features.GeoJsonTooltip(['solarenergy_kwh_Legend'], labels=False) # type: ignore
      )

    # add choropleth
    choropleth.add_to(Carte)
    execution_date = datetime.now()
    timestamp = execution_date.strftime("%Y%m%d-%H%M%S")
    
    #Save carte and display
    Carte.save(f"assets/ensoleillement-departements-{timestamp}.html")
    display(Carte) # type: ignore
    

def test_departments_sunshine_plot(geo_departments):
    """
    Display sunshine and solarenergy(kWh) of department
    """
    import folium # type: ignore
    # Blank Map and create Map object via folium
    location=[47, 1]
    zoom_start = 5
    tiles = "cartodbpositron"

    Carte = folium.Map(location=location,
                      zoom_start=zoom_start,
                      tiles=tiles)
    geo_departments = geo_departments
    # Map visualization
    choropleth = folium.Choropleth(geo_data = geo_departments.to_json(),
                                    data = geo_departments,
                                    columns = ['dep', 'solarenergy_kwh'],
                                    key_on = 'feature.properties.dep',
                                    fill_color = 'OrRd',
                                    fill_opacity = 0.5,
                                    line_opacity = 1,
                                    legend_name = 'Ensoleillement (KWh/m².j)')

    # Add labels containing legend
    choropleth.geojson.add_child(
      folium.features.GeoJsonTooltip(['solarenergy_kwh_Legend'], labels=False) # type: ignore
      )

    #choropleth
    choropleth.add_to(Carte)
    #display
    display(Carte) # type: ignore
