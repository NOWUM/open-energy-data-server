from shapely.geometry import Point


def create_nuts_map(data):
    longitudes, latitudes, geo = data
    nuts = []
    for num in range(5):
        point = Point(longitudes[num], latitudes[num])
        nut = 'x'
        for _, row in geo.iterrows():
            if row['geometry'].contains(point):
                nut = row['NUTS_ID']
                nuts.append(nut)
                break
        if nut == 'x':
            nuts.append('x')

    return nuts
