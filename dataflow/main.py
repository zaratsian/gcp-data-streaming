

################################################################################################################
#
#   Google Cloud Dataflow
#
#   References:
#   https://cloud.google.com/dataflow/docs/
#
#   Usage:
'''
python main.py \
    --gcp_project dz-apps \
    --region us-central1 \
    --job_name 'data-stream' \
    --gcp_staging_location "gs://dz-apps-dataflow/staging" \
    --gcp_tmp_location "gs://dz-apps-dataflow/tmp" \
    --batch_size 10 \
    --pubsub_topic projects/dz-apps/topics/data-stream \
    --runner DirectRunner

'''
#
################################################################################################################


from __future__ import absolute_import
import logging
import argparse
import json
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms import trigger
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from past.builtins import unicode

################################################################################################################
#
#   Variables
#
################################################################################################################

bq_schema = {'fields': [
    {'name': 'uid',         'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'x_cord',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'y_cord',      'type': 'INT64',  'mode': 'NULLABLE'}
]}

################################################################################################################
#
#   Functions
#
################################################################################################################

def parse_pubsub(event):
    return json.loads(event)


def preprocess_event(event):
    return (event['name'], event['value'])


def sum_by_group(GroupByKey_tuple):
      (word, list_of_ones) = GroupByKey_tuple
      return {"name":word, "sum":sum(list_of_ones)}


def avg_by_group(GroupByKey_tuple):
    (k,v) = GroupByKey_tuple
    return {"name":k, "avg": (sum(v) / len(v)) }


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcp_project',          required=True,    default='gaming-demos',       help='GCP Project ID')
    parser.add_argument('--region',               required=True,    default='us-central1',        help='GCP Region')
    parser.add_argument('--job_name',             required=True,    default='antidote-ensemble',  help='Dataflow Job Name')
    parser.add_argument('--gcp_staging_location', required=True,    default='gs://xxxxx/staging', help='Dataflow Staging GCS location')
    parser.add_argument('--gcp_tmp_location',     required=True,    default='gs://xxxxx/tmp',     help='Dataflow tmp GCS location')
    parser.add_argument('--batch_size',           required=True,    default=10,                   help='Dataflow Batch Size')
    parser.add_argument('--pubsub_topic',         required=True,    default='',                   help='Input PubSub Topic: projects/<project_id>/topics/<topic_name>')#parser.add_argument('--bq_dataset_name',      required=True,   default='',                   help='Output BigQuery Dataset')
    #parser.add_argument('--bq_table_name',       required=True,   default='',                    help='Output BigQuery Table')
    parser.add_argument('--runner',               required=True,    default='DirectRunner',       help='Dataflow Runner - DataflowRunner or DirectRunner (local)')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    pipeline_args.extend([
          '--runner={}'.format(known_args.runner),                          # DataflowRunner or DirectRunner (local)
          '--project={}'.format(known_args.gcp_project),
          '--staging_location={}'.format(known_args.gcp_staging_location),  # Google Cloud Storage gs:// path
          '--temp_location={}'.format(known_args.gcp_tmp_location),         # Google Cloud Storage gs:// path
          '--job_name=' + str(known_args.job_name),
      ])
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    
    ###################################################################
    #   DataFlow Pipeline
    ###################################################################
    
    with beam.Pipeline(options=pipeline_options) as p:
        
        # Get PubSub Topic
        logging.info('Ready to process events from PubSub topic: {}'.format(known_args.pubsub_topic)) 
        events = ( 
                 p  | 'raw events' >> beam.io.ReadFromPubSub(known_args.pubsub_topic) 
        )
        
        # Parse events
        parsed_events = (
            events  | 'parsed events' >> beam.Map(parse_pubsub)
        )
        
        # Tranform events
        event_window = (
            parsed_events | 'window' >> beam.Map(preprocess_event)
                           | beam.WindowInto(window.SlidingWindows(30, 5)) # Window is 30 seconds in length, and a new window begins every five seconds
                           | beam.GroupByKey()
                           | beam.Map(avg_by_group)
        )
        
        # Print results to console (for testing/debugging)
        event_window | 'print' >> beam.Map(print)
        
        '''
        # Sink/Persist to BigQuery
        parsed | 'Write to bq' >> beam.io.gcp.bigquery.WriteToBigQuery(
                        table=known_args.bq_table_name,
                        dataset=known_args.bq_dataset_name,
                        project=known_args.gcp_project,
                        schema=bq_schema,
                        batch_size=int(known_args.batch_size)
                        )
        '''
        
        # Sink data to PubSub
        #output | beam.io.WriteToPubSub(known_args.output_topic)


################################################################################################################
#
#   Main
#
################################################################################################################

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()




#ZEND
