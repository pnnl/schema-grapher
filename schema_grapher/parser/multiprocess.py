import time
import os
import multiprocessing
import json
from socket import error as SocketError
import csv
import logging

from schema_grapher.util import ReadCSV, MapHeader
from schema_grapher.util import ProcessParser
from schema_grapher.util.rdf import PropertyType
from schema_grapher.util.s3 import S3

logger = logging.getLogger(__name__)

def ParseConfigMulti(configfile):
    """Reads in a config file and processes the specification in multiple threads"""
    processes = []
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
            logger.info("Parsing " + i['FILE'])
            spec = json.load(open(i['SPEC']))
            t = ReadCSV(i['FILE'])
            fileprefix = i['FILE'].split(os.sep)[-1].split('.')[0]
            th = MapHeader(next(t))

            row = next(t, None)
            rowcount = 0
            rows = []
            trows = []
            fcount = 0
            while row != None:
                rows += [row]
                if rowcount >= config['CHUNKSIZE']:
                    trows += [rows]
                    rows = []
                    rowcount = -1
                rowcount += 1

                row = next(t, None)
                if row == None:
                    trows += [rows]

                while len(trows) > 0:
                    job = trows.pop(0)
                    fname = os.path.join(config['OUTPUTDIR'], fileprefix + '_' + str(fcount) + '.nt')
                    fcount += 1
                    offset = (fcount-1) * config['CHUNKSIZE'] + (fcount-1)
                    process = multiprocessing.Process(target=ProcessParser, args = (fname, spec.copy(), th.copy(), job.copy(), offset, pt, config['DETERMINISTIC_IDS']))
                    processes.append(process)
                    process.start()
                    waiting = True
                    while waiting:
                        time.sleep(1)
                        alivecount = 0
                        for i in range(len(processes)):
                            process = processes.pop(0)
                            if process.is_alive() == False:
                                process.join(5)
                            else:
                                alivecount += 1
                                processes.append(process)

                        if alivecount < config['THREADS']:
                            waiting = False

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

