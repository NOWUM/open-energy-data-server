import pandas as pd
import requests, zipfile, io
import shapefile
from shapely.geometry import shape, Point


if __name__ == "__main__":
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://opendata:opendata@10.13.10.41:5432/scigrid')
    shape3_germany = shapefile.Reader(r'./shapes/plz-5stellig.shp')
    plz_areas_3 = {feature.record['plz']: shape(feature.shape.__geo_interface__)
                  for feature in shape3_germany.shapeRecords()}

    response = requests.get('https://www.power.scigrid.de/releases_archive/scigrid-conference-eu-data-only.zip')
    z = zipfile.ZipFile(io.BytesIO(response.content))
    for file in z.filelist:
        name = file.filename
        if 'links_eu_power_160718.csvdata.xlsx' in name:
            links = pd.read_excel(z.open(name))
        if 'vertices_eu_power_160718.csvdata.xlsx' in name:
            nodes = pd.read_excel(z.open(name))

    areas_1 = []
    areas_2 = []
    for index, row in links.iterrows():
        line = eval(row['wkt_srid_4326'].split(';')[-1].replace('LINESTRING', '').replace(' ', ','))
        coord_1 = Point(line[0], line[1])
        coord_2 = Point(line[2], line[3])
        area_1, area_2 = -1, -1
        for key, area in plz_areas_3.items():
            if area.contains(coord_1):
                area_1 = int(key)
                areas_1.append((index, area_1))
            if area.contains(coord_2):
                area_2 = int(key)
                areas_2.append((index, area_2))
            if area_1 > -1 and area_2 > -1:
                break
    for index, area in areas_1:
        links.at[index, 'plz1'] = area
    for index, area in areas_2:
        links.at[index, 'plz2'] = area
    links.to_sql('edges', engine, if_exists='replace', index=False)

    areas = []
    for index, row in nodes.iterrows():
        point = eval(row['wkt_srid_4326'].split(';')[-1].replace('POINT', '').replace(' ', ','))
        coord_1 = Point(point[0], point[1])
        for key, area in plz_areas_3.items():
            if area.contains(coord_1):
                area = int(key)
                areas.append((index,area))
                break
    for index, area in areas:
        nodes.at[index, 'plz'] = area

    nodes.to_sql('nodes', engine, if_exists='replace', index=False)