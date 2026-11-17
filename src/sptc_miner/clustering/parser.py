'Parser for nine-column OD center point CSV rows.'


def parse_line(line):
    'Parse OD endpoints, weight, coordinates and midpoint longitude/latitude.'
    fields = line.strip().split(',')
    station1 = str(fields[0])
    station2 = str(fields[1])
    weight = float(fields[2])
    lon1 = float(fields[3])
    lat1 = float(fields[4])
    lon2 = float(fields[5])
    lat2 = float(fields[6])
    center_lon = float(fields[7])
    center_lat = float(fields[8])
    return [station1, station2, weight, lon1, lat1, lon2, lat2, center_lon, center_lat]
