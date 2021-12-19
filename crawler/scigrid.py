import pandas as pd
import requests, zipfile, io
import geopandas as gpd
import shapefile
import shapely.wkt
from shapely.geometry import shape, Point


if __name__ == "__main__":
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://opendata:opendata@10.13.10.41:5432/scigrid')

    response = requests.get('https://www.power.scigrid.de/releases_archive/scigrid-conference-eu-data-only.zip')
    z = zipfile.ZipFile(io.BytesIO(response.content))
    for file in z.filelist:
        name = file.filename
        if 'links_eu_power_160718.csvdata.xlsx' in name:
            links = pd.read_excel(z.open(name))
        if 'vertices_eu_power_160718.csvdata.xlsx' in name:
            nodes = pd.read_excel(z.open(name))

    new_columns = list(nodes.columns[:-1])
    new_columns.append('geometry')
    nodes.columns = new_columns

    nodes['geometry'] = [shapely.wkt.loads(point.replace('SRID=4326;', '')) for point in nodes['geometry']]
    nodes = gpd.GeoDataFrame(nodes)
    # nodes.plot()

    new_columns = list(links.columns[:-1])
    new_columns.append('geometry')
    links.columns = new_columns

    links['geometry'] = [shapely.wkt.loads(line.replace('SRID=4326;', '')) for line in links['geometry']]
    links = gpd.GeoDataFrame(links)
    # links.plot()

    eu_nuts = gpd.read_file(r'../shapes/NUTS_EU.shp')
    eu_nuts = eu_nuts[eu_nuts['LEVL_CODE'] == 3]
    eu_nuts = eu_nuts.to_crs(4326)

    areas = []
    for index, link in links.iterrows():
        points = link['geometry']
        points = points.coords.xy
        point_1 = Point((points[0][0], points[1][0]))
        point_2 = Point((points[0][1], points[1][1]))
        a1 = None
        a2 = None
        for _, nut in eu_nuts.iterrows():
            geom = nut['geometry']
            if geom.contains(point_1):
                a1 = nut['NUTS_ID']
            if geom.contains(point_2):
                a2 = nut['NUTS_ID']
            if a1 is not None and a2 is not None:
                break
        areas.append((a1, a2))
    links['areas'] = areas
    links['geometry'] = links['geometry'].to_numpy(str)
    links.to_sql('edges', engine, if_exists='replace', index=False)

    areas = []
    for index, node in nodes.iterrows():
        point = node['geometry']
        for _, nut in eu_nuts.iterrows():
            geom = nut['geometry']
            if geom.contains(point):
                areas.append(nut['NUTS_ID'])
                break
    nodes['area'] = areas
    nodes['geometry'] = nodes['geometry'].to_numpy(str)
    nodes.to_sql('nodes', engine, if_exists='replace', index=False)