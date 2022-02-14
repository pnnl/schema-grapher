import uuid
import hashlib
import datetime
import dateutil
from geographiclib.geodesic import Geodesic

def ResolveBind(binding, row, header):
    """Function that resolves the bind function specified in a dataset annotation"""
    if binding["FUNCTION"] == "GeoJSONFromLatLon":
        return GeoJSONFromLatLon(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "GeoJSONFromCombinedLatLon":
        return GeoJSONFromCombinedLatLon(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "GeoJSONFromCombinedLonLat":
        return GeoJSONFromCombinedLonLat(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "GeoJSONHexFromCombinedLonLat":
        return GeoJSONHexFromCombinedLonLat(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "GeoJSONHexFromCombinedLatLon":
        return GeoJSONHexFromCombinedLatLon(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "DeterministicUUID":
        return DeterministicUUID(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "Echo":
        return Echo(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "CombineColumns":
        return CombineColumns(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "SplitColumn":
        return SplitColumn(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "SplitIndex":
        return SplitIndex(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "TernaryBool":
        return TernaryBool(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "Replace":
        return Replace(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "ProperCase":
        return ProperCase(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "UpCase":
        return UpCase(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "Concatenate":
        return Concatenate(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "OffsetDate":
        return OffsetDate(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "UnixTimestamp":
        return UnixTimestamp(binding["DATA"], row, header)
    elif binding["FUNCTION"] == "ObjectTemplate":
        return ObjectTemplate(binding["DATA"], row, header)
    else:
        raise Exception("Binding Function Not Found")

def GeoJSONFromLatLon(data, row, header):
    """Bind function that returns a geojson object from a Latitude and Longitude. Columns expected in annotation: Latitude and Longitude"""
    if data["Longitude"] in header.keys() and data["Latitude"] in header.keys() and len(row) > header[data['Longitude']] and len(row) > header[data['Latitude']]:
        if row[header[data["Longitude"]]] == "nan" or row[header[data["Latitude"]]] == "nan":
            return None
    try:
        return {"type": "Point", "coordinates": [float(row[header[data["Longitude"]]]), float(row[header[data["Latitude"]]])]}
    except:
        return None

def GeoJSONFromCombinedLatLon(data, row, header):
    """Bind function that returns a geojson object from a Latitude and Longitude. Columns expected in annotation: LatLon (comma delimited latitude and longitude)"""
    try:
        latlon = row[header[data["LatLon"]]].strip("()[]").replace(',', ' ').replace('  ',' ').split(" ")
        return {"type": "Point", "coordinates": [float(latlon[1]), float(latlon[0])]}
    except:
        return None

def GeoJSONFromCombinedLonLat(data, row, header):
    """Bind function that returns a geojson object from a Latitude and Longitude. Columns expected in annotation: LonLat (comma delimited longitude followed by latitude)"""
    try:
        latlon = row[header[data["LonLat"]]].strip("()[]").replace(',', ' ').replace('  ', ' ').split(" ")
        return {"type": "Point", "coordinates": [float(latlon[0]), float(latlon[1])]}
    except:
        return None

def PolygonLonLat(lat, lon, numvertex, distance):
    """Function to determine where points on a polygon should be placed in geocoordinate space"""
    geod = Geodesic.WGS84
    angle = 360 / numvertex
    coords = []
    for i in range(numvertex-1,-1,-1):
        vertex = geod.Direct(lat, lon, float(angle*i), distance)
        coords += [[vertex['lon2'], vertex['lat2']]]
    coords += [coords[0]]
    return coords

def GeoJSONHexFromCombinedLonLat(data, row, header):
    """Bind function that returns a geojson object containing a hexagon polygon with a specified radius centered on a Latitude and Longitude. Columns expected in annotation: LonLat (comma delimited longitude followed by latitude), Radius (the outer radius of the polygon)"""
    try:
        latlon = row[header[data["LonLat"]]].strip("()[]").replace(',', ' ').replace('  ', ' ').split(" ")
        return {"type": "Polygon", "coordinates": [PolygonLonLat(float(latlon[0]), float(latlon[1]), 6, data['Radius'])]}
    except:
        return None

def GeoJSONHexFromCombinedLatLon(data, row, header):
    """Bind function that returns a geojson object containing a hexagon polygon with a specified radius centered on a Latitude and Longitude. Columns expected in annotation: LatLon (comma delimited latitude followed by longitude), Radius (the outer radius of the polygon)"""
    try:
        latlon = row[header[data["LatLon"]]].strip("()[]").replace(',', ' ').replace('  ', ' ').split(" ")
        return {"type": "Polygon", "coordinates": [PolygonLonLat(float(latlon[1]), float(latlon[0]), 6, data['Radius'])]}
    except:
        return None

def DeterministicUUID(data, row, header):
    """Bind function that returns a deterministic UUID. Columns expected in annotation: Identifier (column to encode), Salt (a string to salt the hash)"""
    return str(uuid.UUID(hashlib.md5((row[header[data["Identifier"]]] + data["Salt"]).encode("utf-8")).hexdigest()))

def Echo(data, row, header):
    """Bind function that returns a defined string. Columns expected in annotation: Echostring (a string to be returned)"""
    return data["Echostring"]

def CombineColumns(data, row, header):
    """Bind function to combine a set of columns seperated by a delimiter. Columns expected in annotation: Columns (a list of columns to combine), Delimeter (a string to delimit the combined output)"""
    return data["Delimiter"].join([row[header[i]] for i in data["Columns"]])

def SplitColumn(data, row, header):
    """Bind function to split a column with a delimeter returns a list. Columns expected in annotation: Column (the column to be split), Delimiter (the string to split the column on)"""
    return row[header[data["Column"]]].split(data['Delimiter'])

def SplitIndex(data, row, header):
    """Bind function to split a column with a delimeter and return a specific element of the list. Columns expected in annotation: Column (the column to be split), Delimiter (the string to split the column on), Index (the index of the list to be returned)"""
    try:
        return SplitColumn(data, row, header)[data['Index']]
    except:
        return ""

def TernaryBool(data, row, header):
    """Bind function that tests a column for a value and returns a boolean. Columns expected in annotation: Column (the column to be tested), True (the string value to be tested against"""
    return True if row[header[data["Column"]]] == data["True"] else False

def Replace(data, row, header):
    """Bind function to replace a string in a column with a specified value. Columns expected in annotation: Column (the column to be searched and replaced), Key (the string to be searched for), Value (the string to replace the Key when found)"""
    return row[header[data["Column"]]].replace(data["Key"], data["Value"])

def ProperCase(data, row, header):
    """Bind function to ensure first character of string is capitalized. Columns expected in annotation: Column (the column with the string to be proper cased)"""
    value = row[header[data["Column"]]].lower()
    return value[0].upper() + value[1:]

def UpCase(data, row, header):
    """Bind function to change case of string to upper case. Columns expected in annotation: Column (the column with the string to upper case)"""
    return row[header[data["Column"]]].upper()

def Concatenate(data, row, header):
    """Bind function to concatenate two columns. Columns expected in annotation: ColumnA (the first column to be concatenated), ColumnB (the second column to be concatenated)"""
    return row[header[data["ColumnA"]]] + row[header[data["ColumnB"]]]

def OffsetDate(data, row, header):
    """Bind function to shift the date of a column by a number of seconds, returns a datetime object. Columns expected in annotation: DateCol (the column with the date), Offset (the number of seconds to shift the DateCol)"""
    if row[header[data["DateCol"]]].replace(' ', '') != '':
        try:
            return datetime.datetime.fromtimestamp(datetime.datetime.timestamp(dateutil.parser.parse(row[header[data["DateCol"]]])) + float(data["Offset"]))
        except:
            return None
    else:
        return None

def UnixTimestamp(data, row, header):
    """Bind function to convert a unix timestamp to a string representation. Columns expcted in annotation: DateCol (the column with the unix timestamp)"""
    try:
        return str(datetime.datetime.utcfromtimestamp(int(row[header[data["DateCol"]]])))
    except:
        return None

def ObjectTemplate(data, row, header):
    """Bind function to construct a dictionary object. Columns expected in annotation: Obj (the template of the output dictionary), Map (a dictionary where the key is a key in the Obj and the value is a column)"""
    nObj = data["Obj"]
    for k,v in data["Map"].items():
        nObj[k] = row[header[v]]
    return nObj
