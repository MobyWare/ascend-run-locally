# TODO: add this for using this on Ascend
"""
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp/pypkg", "ascend-io-sdk"])
sys.path.append("/tmp/pypkg")
"""

import os
from ascend.sdk.client import Client
from google.protobuf import json_format
from pyspark.sql import SparkSession, DataFrame
import pyspark
import pyspark.sql.types as T
from typing import List

import logging as log
log.basicConfig(level=log.INFO)

PAGE_SIZE = 100
MAX_PARTITIONS = 500

output_schema = T.StructType([
    T.StructField('data_service_id', T.StringType(), False),
    T.StructField('dataflow_id', T.StringType(), False),
    T.StructField('component_id', T.StringType(), False),
    T.StructField('log_sequence_number', T.LongType(), False),
    T.StructField('log_line', T.StringType(), False)    
])


def infer_schema(spark_session: SparkSession, inputs: List[DataFrame], credentials=None) -> T.StructType:
    return output_schema


def get_logs(client, component, log_request, next_page_cursor=None):
    log.info('log_request %s',log_request)

    #TODO: believe range_start/range_end is a unixtimestamp with nanoseconds
    log_data = client.get_logs(range_start=log_request['rangeStart'], range_end=log_request['rangeEnd'], label=log_request['label'], jwt=log_request['jwt'], next_page_cursor=next_page_cursor)
    
    for r in log_data.data.result:
        for v in r.values:
            # 0 is a log sequence number, 1 is log line
            yield {
                'data_service_id': component['data_service_id'], 
                'dataflow_id': component['dataflow_id'], 
                'component_id': component['component_id'],
                'log_sequence_number': int(v[0]), #unix timestamp w/ nanoseconds
                'log_line': v[1]
            }

    next_page_cursor = log_data.data.next_page_cursor
    log.debug('Next page cursor is %s', next_page_cursor)

    if next_page_cursor:
        log.info('Needed to retrieve more logs...')
        for line in get_logs(client, component, log_request, next_page_cursor):
            yield line

def scrape_components(client, component_list):
    #component_list = dict(data_service_id, dataflow_id, component_id, component_type)
    limit = PAGE_SIZE

    for component in component_list:
        data_service_id = component['data_service_id']
        dataflow_id = component['dataflow_id']
        component_id = component['component_id']
        component_type = component['component_type']
        
        for offset in range(0,MAX_PARTITIONS,PAGE_SIZE):
            # TODO: add the code to break if no result comes back from the API
            if component_type == 'read':
                partitions = client.get_read_connector_partitions(data_service_id=data_service_id, dataflow_id=dataflow_id, id=component_id, limit=limit, offset=offset)
            elif component_type == 'transform':
                partitions = client.get_transform_partitions(data_service_id=data_service_id, dataflow_id=dataflow_id, id=component_id, limit=limit, offset=offset)
            elif component_type == 'write':
                partitions = client.get_write_connector_partitions(data_service_id=data_service_id, dataflow_id=dataflow_id, id=component_id, limit=limit, offset=offset)

            part_data = partitions.data.data.metadata
            for p in part_data:
                # we have partition results
                log_request = json_format.MessageToDict(p.log_request)
                for logs in get_logs(client, component, log_request):
                    yield logs

# main/transform
def transform(spark_session: SparkSession, inputs: List[DataFrame], credentials=None) -> DataFrame: 
    c = Client(os.environ.get('ASCEND_HOSTNAME'))
    
    # TODO: use the API to create an iterable of component_list to scrape, 
    # maybe in list objects and pass in as dataframe, with a collect
    # TODO: add a filter override
    # TODO: change client to primitives so that this can be parallelized/map partitions to get contents

    component_list = [
        {
            'data_service_id':os.environ.get('ASCEND_DATASERVICE','Steady'),
            'dataflow_id':os.getenv('ASCEND_DATAFLOW'),
            'component_id': os.getenv('ASCEND_COMPONENT'),
            'component_type':os.getenv('ASCEND_COMPONENT_TYPE')
        }
    ]
    
    return spark.createDataFrame(
        scrape_components(client=c, component_list=component_list), 
        schema=output_schema
    )


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[1]") \
        .getOrCreate()
    # spark.sparkContext.setLogLevel("ERROR")

    #TODO: fake a dataframe that lists data service id, flow id and component id

    # simulating ascend calls    
    infer_schema(spark, [], None)
    transform(spark, [], None).show()

    