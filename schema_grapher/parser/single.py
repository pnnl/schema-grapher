import os
import json
from socket import error as SocketError
import csv
import logging
from schema_grapher.util.s3 import S3

from schema_grapher.util import ReadCSV, MapHeader
from schema_grapher.util.rdf import RenderTriples, RowTriples, PropertyType

logger = logging.getLogger(__name__)

def ParseConfigSingle(configfile):
    """Reads in a config file and processes the specification in a single thread"""
    config = json.load(open(configfile))
    for k,v in dict(os.environ).items():
        try:
            config[k] = json.loads(v)
        except:
            config[k] = v
    if 'SCHEMA' in list(config.keys()):
        pt = PropertyType(config['SCHEMA'])
    else:
        pt = {}
    for i in config['FILES']:
        if os.path.exists(i['FILE']) and os.path.exists(i['SPEC']):
            logger.info("Parsing {} and generating triples".format(i['FILE']))
            spec = json.load(open(i['SPEC']))
            t = ReadCSV(i['FILE'])
            fileprefix = i['FILE'].split(os.sep)[-1].split('.')[0]
            th = MapHeader(next(t))

            rowcount = 0
            fcount = 0
            fname = os.path.join(config['OUTPUTDIR'], fileprefix + '_' + str(fcount) + '.nt')
            f = open(fname, 'w')
            for cnt, j in enumerate(t):
                if rowcount > config['CHUNKSIZE']:
                    f.close()
                    rowcount = 0
                    fcount += 1
                    fname = os.path.join(config['OUTPUTDIR'], fileprefix + '_' + str(fcount) + '.nt')
                    f = open(fname, 'w')
                if j != "":
                    f.write(RenderTriples(RowTriples(spec, th, j, os.path.split(fname)[1], cnt, pt, deterministic = config['DETERMINISTIC_IDS'])))
                rowcount += 1
            f.close()


            # prepend S3 folder structure to filename with path removed
            s3_obj_name = config['S3_FOLDER'] + os.path.split(fname)[1]

            # Connect to AWS S3 and Upload
            logger.info("Connecting to AWS S3.")
            sss = S3()
            logger.info("Uploading {} to S3".format(s3_obj_name))
            sss.upload_file(fname, s3_obj_name, zip=True)

        else:
            if not os.path.exists(i['FILE']):
                logger.error("File not found: " + i['FILE'])
            if not os.path.exists(i['SPEC']):
                logger.error("File not found: " + i['SPEC'])
