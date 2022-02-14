import os
import copy
import uuid
import hashlib
import rdflib
import json
import datetime
import dateutil
import sys
import logging
import urllib

logger = logging.getLogger(__name__)

RDFTYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
RDFCLASS = 'http://www.w3.org/2000/01/rdf-schema#Class'
RDFPROPERTY = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#Property'
RDFSUBCLASS = 'http://www.w3.org/2000/01/rdf-schema#subClassOf'
RDFDOMAIN = 'http://schema.localhost/domainIncludes'
RDFRANGE = 'http://schema.localhost/rangeIncludes'
RDFDATATYPE = 'http://schema.localhost/DataType'
RDFNS = 'http://schema.localhost/'

from schema_grapher.util.bind import ResolveBind
from schema_grapher.util.misc import AddressQuery, LatLonQuery

def PropertyType(schema_jsonld):
    """Generates a dictionary mapping each attribute of the schema to it's basic type"""
    pt = {}
    schema = None
    if 'http://' in schema_jsonld:
        try:
            qr = urllib.request.urlopen(schema_jsonld)
            schema = json.loads(qr.read().decode())
        except Exception as e:
            logger.error("Could not retrieve schema from external server", exc_info=e)
    elif os.path.exists(schema_jsonld):
        with open(schema_jsonld) as f:
            schema = json.load(f)
    if type(schema) is dict and '@graph' in schema.keys():
        pt = {i['@id'].replace(RDFNS, '') : i[RDFNS + 'rangeIncludes']['@id'].replace(RDFNS, '') for i in schema['@graph'] if RDFNS + 'rangeIncludes' in i.keys()}
    return pt

def ProcessParser(fname, spec, header, rows, offset, pt, deterministic = None):
    """This function parses a set of rows to RDF and writes to the defined output file"""
    f = open(fname, 'w')
    for cnt, i in enumerate(rows):
        if i != "":
            f.write(RenderTriples(RowTriples(spec, header, i, os.path.split(fname)[1], cnt + offset, pt, deterministic = deterministic)))
    f.close()

def ParseDatum(prop, datum, pt):
    """This function attempts to properly types a datum depending on the type of attribute it is in the schema"""
    try:
        pdat = datum
        if prop in pt.keys():
            #if schema is availabe use schema attribute type for datum
            if pt[prop] == 'DateTime':
                if type(datum) is datetime.datetime:
                    pdat = WrapDQ(datum)
                else:
                    pdat =  WrapDQ(dateutil.parser.parse(datum))
            elif pt[prop] == 'Decimal':
                pdat = WrapDQ(float(datum))
            elif pt[prop] == 'String':
                pdat = WrapDQ(str(datum))
            elif pt[prop] == 'Integer':
                pdat = WrapDQ(int(datum))
            elif pt[prop] == 'DecimalList':
                pdat = WrapDQ(float(datum))
            elif pt[prop] == 'StringList':
                pdat = WrapDQ(str(datum))
            elif pt[prop] == 'IntegerList':
                pdat = WrapDQ(int(datum))
            elif pt[prop] == 'DecimalSet':
                pdat = WrapDQ(float(datum))
            elif pt[prop] == 'StringSet':
                pdat = WrapDQ(str(datum))
            elif pt[prop] == 'IntegerSet':
                pdat = WrapDQ(int(datum))
            elif pt[prop] == 'Boolean':
                pdat = WrapDQ(bool(datum))
            elif pt[prop] == 'GeoJSON':
                pdat = WrapDQ(str(json.dumps(datum))) + "^^" + WrapNS("GeoJSON")
            elif prop == 'metaData':
                pdat = WrapDQ(str(json.dumps(datum)))
            else:
                pdat = WrapDQ(str(datum)) + "^^" + WrapNS(pt[prop])
            return pdat
        else:
            #if schema not available default to using datum type
            if type(datum) is datetime.datetime:
                pdat = WrapDQ(datum)
            elif type(datum) is float:
                pdat = WrapDQ(float(datum))
            elif type(datum) is str:
                pdat = WrapDQ(str(datum))
            elif type(datum) is int:
                pdat = WrapDQ(int(datum))
            elif type(datum) is bool:
                pdat = WrapDQ(bool(datum))
            elif type(datum) is dict:
                pdat = WrapDQ(str(json.dumps(datum))) + "^^" + WrapNS("GeoJSON")
            return pdat
    except:
        
        return None

def DeterministicGensym(deterministic, rowplus, headerplus, template, fname):
    """This function generates UUID for the gensyms in an annotation template based on their dependency relationship"""
    ntemplate = []
    for i in template:
        gensym = [v for k,v in i[1].items() if k == "GENSYM"][0]
        ntemplate += [[gensym, [k if "GENSYM_" in k else ((rowplus[headerplus[k]] if rowplus[headerplus[k]] != None else '')if len(rowplus)-1 >= headerplus[k] else '') for k,v in i[1].items() if k != "GENSYM"]]]
        
    gsMap = {i[0] : None for i in ntemplate}
    
    while(None in gsMap.values()):
        obj = ntemplate.pop()
        resolved = True
        for ix,i in enumerate(obj[1]):
            if "GENSYM_" in i:
                if gsMap[i.split('_')[-1]] != None:
                    obj[1][ix] = gsMap[i.split('_')[-1]]
                else:
                    resolved = False
        if resolved == True:
            if deterministic == "GLOBAL":
                gsMap[obj[0]] = str(uuid.UUID(hashlib.md5(json.dumps(obj[1]).encode("utf-8")).hexdigest()))
            elif deterministic == "FILE":
                gsMap[obj[0]] = str(uuid.UUID(hashlib.md5((json.dumps(obj[1]) + fname).encode("utf-8")).hexdigest()))
        ntemplate.insert(0,obj)
            
    return gsMap
        
def RowTriples(spec, header, row, fname, cnt, pt, gsMap = None, subiter = None, deterministic = None):
    """This function parses a row of data"""
    template = copy.deepcopy(spec["TEMPLATE"])
    rowplus = list(copy.deepcopy(row))
    headerplus = header
    gensymMap = copy.deepcopy(gsMap) if gsMap is not None else {}
    triples = []
    ltriples = []
    subiterval = str(subiter) if subiter is not None else ""

    for j in spec['BIND'].keys():
        rowplus += [ResolveBind(spec['BIND'][j], rowplus, headerplus)]
        headerplus[j] = len(rowplus)-1
    if deterministic == None or deterministic == "NONE":
        for j in template:
            if j[1]['GENSYM'] in gensymMap.keys():
                pass
            elif 'BIND' in j[1]['GENSYM']:
                gensymMap[j[1]['GENSYM']] = rowplus[headerplus[j[1]['GENSYM']]]
            elif "HASHED_IDS" in spec['OPTIONS'].keys() and spec['OPTIONS']['HASHED_IDS'] == True:
                gensymMap[j[1]['GENSYM']] = str(uuid.UUID(hashlib.md5((subiterval + j[1]['GENSYM'] + fname + str(cnt)).encode("utf-8")).hexdigest()))
            else:
                gensymMap[j[1]['GENSYM']] = str(uuid.uuid4())
    else:
        gensymMap = DeterministicGensym(deterministic, rowplus, headerplus, template, fname)
    for j in template:
        if j[0] == 'AddressLocation':
            classID = WrapNS(gensymMap[j[1]['GENSYM']])
            if "GEOLOOKUP" in spec["OPTIONS"] and spec['OPTIONS']['GEOLOOKUP'] == True:
                qaddress = AddressQuery(BuildAddress({v[0] : rowplus[headerplus[k]] for k,v in j[1].items() if k != "GENSYM"}))
                if 'features' in qaddress.keys() and len(qaddress['features']) > 0 and qaddress['features'][0]['properties']['place_rank'] > 20:
                    ltriples += AddressTriples(classID, qaddress['features'][0], pt)
            #j[1] = {'GENSYM' : j[1]['GENSYM']}
    for j in template:
        classID = WrapNS(gensymMap[j[1]['GENSYM']])
        for k,v in j[1].items():
            if k == "GENSYM":
                triples += [(classID, "<" + RDFTYPE + ">", WrapNS(j[0]))]
            else:
                for a in v:
                    if "SUBTEMPLATE" in k:
                        subgensym = k.split('.')[1].split('_')[-1]
                        subiter = spec["SUBTEMPLATES"][k.split('.')[0].split('_')[-1]]["ITERABLE"]
                        for ix, x in enumerate(rowplus[headerplus[subiter[0][0]]]):
                            if 'BIND' in subgensym:
                                gensymMap[subgensym] = rowplus[headerplus[subgensym]]
                            elif "HASHED_IDS" in spec['OPTIONS'].keys() and spec['OPTIONS']['HASHED_IDS'] == True:
                                gensymMap[subgensym] = str(uuid.UUID(hashlib.md5((str(ix) + subgensym + fname + str(cnt)).encode("utf-8")).hexdigest()))
                            else:
                                gensymMap[subgensym] = str(uuid.uuid4())
                            if subiter[0][1] in headerplus.keys():
                                rowplus[headerplus[subiter[0][1]]] = x
                            else:
                                rowplus += [x]
                                headerplus[subiter[0][1]] = len(rowplus)-1
                            triples += [(classID, WrapNS(a), WrapNS(gensymMap[subgensym]))]
                            triples += RowTriples(spec["SUBTEMPLATES"][k.split('.')[0].split('_')[-1]], headerplus, rowplus, fname, cnt, pt, gensymMap, ix, deterministic)
                            headerplus.pop(subiter[0][1], None)
                    elif "GENSYM_" in k:
                        triples += [(classID, WrapNS(a), WrapNS(gensymMap[k.split('_')[-1]]))]
                    else:
                        try:
                            datum = rowplus[headerplus[k]]
                        except:
                            datum = ''
                        if datum == "":
                            pass
                        elif type(datum) is list:
                            for b in datum:
                                pdatum = ParseDatum(a, b, pt)
                                if pdatum is not None:
                                    triples += [(classID, WrapNS(a), pdatum)]
                        elif datum != None:
                            pdatum = ParseDatum(a, datum, pt)
                            if pdatum is not None:
                                triples += [(classID, WrapNS(a), pdatum)]
                #triples += [(classID, WrapNS(a), ParseDatum(a,rowplus[headerplus[k]])) if "GENSYM" not in a else (classID, WrapNS(k), WrapNS(gensymMap[a.split('_')[-1]])) for a in v if "GENSYM" in a or ParseDatum(a,rowplus[headerplus[k]]) != None]
    lltriples = []
    for i in ltriples:
        found = False
        for j in triples:
            if i[0] == j[0] and i[1] == j[1]:
                found = True
        if found == False:
            lltriples += [i]

    triples += lltriples
    return triples

def BuildAddress(adict):
    """This function constructs an address for use in an address query"""
    address = []
    keys = adict.keys()
    if 'locationAddressFullText' in keys:
        return "q=" + urllib.parse.quote_plus(adict['locationAddressFullText'])
    street = []
    if 'locationStreetNumberText' in keys:
        street += [adict['locationStreetNumberText']]
    if 'locationStreet' in keys:
        street += [adict['locationStreet']]
    if len(street) > 0:
        address += ["street=" + urllib.parse.quote_plus(' '.join(street))]
    if 'locationCity' in keys:
        address += ["city=" + urllib.parse.quote_plus(adict['locationCity'])]
    if 'locationState' in keys:
        address += ["state=" + urllib.parse.quote_plus(adict['locationState'])]
    if 'locationPostalCode' in keys:
        address += ["postalcode=" + urllib.parse.quote_plus(adict['locationPostalCode'])]
    if 'locationCountry' in keys:
        address += ["country=" + urllib.parse.quote_plus(adict['locationCountry'])]
    return '&'.join(address)

def AddressTriples(locID, qdict, pt, ns = "http://schema.localhost"):
    """This function parses the return from a nominatim query into schema specific attributes"""
    keys = qdict['properties'].keys()
    if 'error' in keys:
        return []
    akeys = qdict['properties']['address'].keys() if type(qdict['properties']['address']) is dict else {}
    triples = []
    if 'display_name' in keys and qdict["properties"]['display_name'] != "":
        triples += [(locID, WrapNS("locationAddressFullText"), WrapDQ(qdict["properties"]['display_name']))]
    if 'city' in akeys:
        triples += [(locID, WrapNS("locationCity"), WrapDQ(qdict["properties"]['address']['city']))]
    if 'country' in akeys:
        triples += [(locID, WrapNS("locationCountry"), WrapDQ(qdict["properties"]['address']['country']))]
    if 'county' in akeys:
        triples += [(locID, WrapNS("locationCounty"), WrapDQ(qdict["properties"]['address']['county']))]
    if 'postcode' in akeys:
        triples += [(locID, WrapNS("locationPostalCode"), WrapDQ(qdict["properties"]['address']['postcode']))]
    if 'state' in akeys:
        triples += [(locID, WrapNS("locationState"), WrapDQ(qdict["properties"]['address']['state']))]
    if 'road' in akeys:
        triples += [(locID, WrapNS("locationStreet"), WrapDQ(qdict["properties"]['address']['road']))]
    if 'house_number' in akeys:
        triples += [(locID, WrapNS("locationStreetNumberText"), WrapDQ(qdict["properties"]['address']['house_number']))]

    if 'geometry' in qdict.keys():
        triples += [(locID, WrapNS("locationGeoPoint"), ParseDatum("locationGeoPoint", qdict['geometry'], pt))]

    return triples

def RenderTriples(triples):
    """This function renders a list of triples to a N-triple string"""
    try:
        return "\n".join([" ".join(i) + ' .' for i in triples]) + "\n"
    except Exception as e:
        logger.error('Failed to render Triple', exc_info=e)
        return ""
    
def StripNS(s):
    """This function strips the namespace from a string"""
    return s.split('/')[-1]

def WrapNS(s, ns = "http://schema.localhost"):
    """This function wraps a string with a specified namespace"""
    return "<" + ns + "/" + s + ">"

def WrapDQ(s):
    """This function renders a variable to it's appropriate N-triple type"""
    return rdflib.Literal(s).n3()
