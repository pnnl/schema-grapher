import json
import codecs
import urllib
import random
import csv
import os
import hashlib
import logging

logger = logging.getLogger(__name__)

def AddressQuery(q, saddress = 'https://nominatim.openstreetmap.org'):
    """Function to geolocate an address using a nominatim server"""
    nominatim = saddress.rstrip('/') + '/search?format=geojson&limit=1&addressdetails=1&'
    try:
        logger.debug("Calling Address query with data:" + nominatim + q)
        qr = urllib.request.urlopen(nominatim + q)
    except:
        # the 500 errors I was seeing seem to be related to https://github.com/osm-search/Nominatim/pull/1220 (EKS)
        logger.error('Nominatim returned a 500 HTTP error for this url (skipping this line of data):' + nominatim + q)
        return {}
    data = json.loads(qr.read())
    return data

def LatLonQuery(lat, lon, saddress = 'https://nominatim.openstreetmap.org'):
    """Function to perform a reverse lookup on a latitude and longitude and determine street address."""
    nominatim = saddress.rstrip('/') + '/reverse?format=geojson&zoom=18&addressdetails=1'
    qr = urllib.request.urlopen(nominatim + '&lat=' + urllib.parse.quote_plus(str(lat)) + '&lon=' + urllib.parse.quote_plus(str(lon)))
    data = json.loads(qr.read())
    return [data] if type(data) == dict else data

def ReadCSV(rawfile):
    """Function to read a CSV lazy"""
    with codecs.open(rawfile, 'r', encoding='utf-8', errors='ignore') as f:
        csv_reader = csv.reader(f, delimiter=',', quotechar='"')
        try:
            for row in csv_reader:
                if "".join(row) != "":
                    yield CleanRow(row)
        except:
            logger.error("Missed Row: " + rawfile)
            yield ""

def CleanRow(row):
    """Function to clean a CSV row of various control characters"""
    return [i.strip("\ufeff").replace("\r", " ").replace("\n", " ") for i in row]
            
def MapHeader(header):
    """Function that returns a dictionary mapping the column header to its index"""
    return {header[i] : i for i in range(len(header))}

def generate_md5(local_filepath):
    """Function to generate the md5 hash of a file"""
    hasher = hashlib.md5()
    with open(local_filepath, 'rb') as rf:
        for chunk in iter(lambda: rf.read(4096), b""):
            hasher.update(chunk)
    final_hash = hasher.hexdigest()
    return final_hash

