from copy import error
from types import TracebackType
import pandas as pd
import re
import urllib.request as ur
import gzip
import json
from urllib.parse import urlparse
import os
import ast

import logger_handler
from database_class import load_ids
from database_class import bulk_insert

# define parameters
loop_size = 200
price_category = ["cheep","normal","expensive","null price"]
computed_column = "dim_price"

        
my_logger = logger_handler.get_logger("processing")

def dim_product():
    url = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Musical_Instruments.json.gz'
    pk_id = 'asin'
    processing(url,pk_id)

def fact_review():
    url = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Musical_Instruments_5.json.gz'
    pk_id = 'reviewerID,asin'
        
    processing(url,pk_id)

# processing function give some parameters that can be set in Airflow then read gzip file on steam
# set table name from url
# unzip file
# check duplicate ids
# fill 200 row in list then call bulk insert function
def processing(url,pk_id):
    lines_collected_temp = list()
    handle = ur.urlopen(url)
    my_logger.debug("get table name from url")
    tableName = os.path.basename(urlparse(url).path).split('.')[0]
    my_logger.debug("fill list of inserted ids to avoid duplicate insert")
    IDs = load_ids(tableName,pk_id)
    print(IDs)
    my_logger.debug("unzip file and read")
    with gzip.GzipFile(fileobj=handle,) as f:
        for line in f:
           
            str_line = line.decode('utf-8')
            json_line = ast.literal_eval(str_line)
            
            my_logger.debug("check price column and add dim_price as a price category")
            if 'price' in json_line :
                if json_line['price'] < 10 :
                    json_line[computed_column] = price_category[0]
                elif json_line['price'] >= 60 :
                    json_line[computed_column] = price_category[2]
                else :
                    json_line[computed_column] = price_category[1]
            else :
                json_line[computed_column] = price_category[3]

            my_logger.debug("fetch data from database to check duplicate IDs")
            id = get_id_from_json_line(json_line,pk_id)
            # if [json_line[pk_id], json_line['asin']] not in IDs:
            if  id not in IDs:
                IDs.append(id)
                lines_collected_temp.append(json_line)
                my_logger.debug("call bulk insert after reading " + str(loop_size) +" rows")
                if len(lines_collected_temp) >= loop_size:
                    bulk_insert(lines_collected_temp, tableName)
                    my_logger.debug("clear list for next batch")
                    lines_collected_temp = list()
                    
def get_id_from_json_line(json_line,pk_id):
    pk_id_array = pk_id.split(',')
    pk_ids = []
    for i in pk_id_array:
        pk_ids.append(json_line[i])
    return pk_ids

    
# dim_product()
