from shapely.geometry import Point


def create_nuts_map(data):
    longitudes, latitudes, geo = data
    nuts = []
    for num in range(len(longitudes)):
        point = Point(longitudes[num], latitudes[num])
        nut = None
        for _, row in geo.iterrows():
            if row['geometry'].contains(point):
                nut = row['NUTS_ID']
                nuts.append(nut)
                break
        if nut is None:
            nuts.append(nut)

    return nuts
