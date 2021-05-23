

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
    --gcp_project gaming-demos \
    --region us-central1 \
    --job_name 'antidote-ensemble' \
    --gcp_staging_location "gs://gaming-demos-antidote-dataflow/staging" \
    --gcp_tmp_location "gs://gaming-demos-antidote-dataflow/tmp" \
    --batch_size 10 \
    \#--pubsub_topics 'antidote-toxicity,antidote-griefing' \
    --topic_antidote_toxicity projects/gaming-demos/topics/antidote-toxicity \
    --topic_antidote_griefing projects/gaming-demos/topics/antidote-griefing \
    --topic_antidote_cheat projects/gaming-demos/topics/antidote-cheat \
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
    {'name': 'game_id',     'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'game_server', 'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'game_type',   'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'game_map',    'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'event_datetime',   'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'player',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'killed',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'weapon',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'x_cord',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'y_cord',      'type': 'INT64',  'mode': 'NULLABLE'}
]}

bq_schema = {'fields': [
    {'name': 'user',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'date',      'type': 'STRING',  'mode': 'NULLABLE'},	
    {'name': 'sessionID',      'type': 'STRING',  'mode': 'NULLABLE'},	
    {'name': 'minutesPlayed',      'type': 'INT64',  'mode': 'NULLABLE'},	
    {'name': 'kills',      'type': 'INT64',  'mode': 'NULLABLE'},	
    {'name': 'weapon',      'type': 'STRING',  'mode': 'NULLABLE'},
    {'name': 'blocksBlasted',      'type': 'INT64',  'mode': 'NULLABLE'},	
    {'name': 'powerUpsUsed',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'moves',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'coins',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'rank',      'type': 'INT64',  'mode': 'NULLABLE'},
    {'name': 'purchases',      'type': 'FLOAT64',  'mode': 'NULLABLE'},	
    {'name': 'friends',      'type': 'INT64',  'mode': 'NULLABLE'}
]}

################################################################################################################
#
#   Functions
#
################################################################################################################

def parse_pubsub(event):
    return json.loads(event)


def preprocess_event(event):
    return (event['userid'], event['score'])


def extract_weapon_type(event):
    return event['weapon']


def sum_by_group(GroupByKey_tuple):
      (word, list_of_ones) = GroupByKey_tuple
      return {"word":word, "count":sum(list_of_ones)}


def avg_by_group(GroupByKey_tuple):
    (k,v) = GroupByKey_tuple
    return {"userid":k, "score": (sum(v) / len(v)) }


def run(argv=None):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcp_project',          required=True,    default='gaming-demos',       help='GCP Project ID')
    parser.add_argument('--region',               required=True,    default='us-central1',        help='GCP Region')
    parser.add_argument('--job_name',             required=True,    default='antidote-ensemble',  help='Dataflow Job Name')
    parser.add_argument('--gcp_staging_location', required=True,    default='gs://xxxxx/staging', help='Dataflow Staging GCS location')
    parser.add_argument('--gcp_tmp_location',     required=True,    default='gs://xxxxx/tmp',     help='Dataflow tmp GCS location')
    parser.add_argument('--batch_size',           required=True,    default=10,                   help='Dataflow Batch Size')
    parser.add_argument('--pubsub_topics',        required=False,   default='',                   help='Comma seperated list of pubsub topic names, ie. antidote-toxicity,antidote-griefing,antidote-cheat')
    parser.add_argument('--topic_antidote_toxicity',required=False, default='',                   help='Input PubSub Topic: projects/<project_id>/topics/<topic_name>')
    parser.add_argument('--topic_antidote_griefing',required=False, default='',                   help='Input PubSub Topic: projects/<project_id>/topics/<topic_name>')
    parser.add_argument('--topic_antidote_cheat',required=False,    default='',                   help='Input PubSub Topic: projects/<project_id>/topics/<topic_name>')
    #parser.add_argument('--bq_dataset_name',      required=True,   default='',                   help='Output BigQuery Dataset')
    #parser.add_argument('--bq_table_name',        required=True,   default='',                   help='Output BigQuery Table')
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
        
        #pubsub_topics = (known_args.pubsub_topics).split(',')
        #pubsub_topics = ['projects/{}/topics/{}'.format(known_args.gcp_project, topic) for topic in pubsub_topics]
        #print('[ INFO ] Pubsub Topics: {}'.format(pubsub_topics))
        
        # Toxicity Topic
        logging.info('Ready to process events from PubSub topic: {}'.format(known_args.topic_antidote_toxicity)) 
        toxicity_raw = ( 
                 p  | 'raw toxicity' >> beam.io.ReadFromPubSub(known_args.topic_antidote_toxicity) 
        )
        
        # Griefing Topic
        logging.info('Ready to process events from PubSub topic: {}'.format(known_args.topic_antidote_griefing))
        griefing_raw = (
                 p  | 'raw griefing' >> beam.io.ReadFromPubSub(known_args.topic_antidote_griefing)
        )
        
        # Cheat Topic
        logging.info('Ready to process events from PubSub topic: {}'.format(known_args.topic_antidote_cheat))
        cheat_raw = (
                 p  | 'raw cheat' >> beam.io.ReadFromPubSub(known_args.topic_antidote_cheat)
        )
        
        # Parse events
        parse_toxicity = (
            toxicity_raw  | 'parse toxicity' >> beam.Map(parse_pubsub)
        )
        
        parse_griefing = (
            griefing_raw  | 'parse griefing' >> beam.Map(parse_pubsub)
        )
        
        parse_cheat = (
            cheat_raw     | 'parse cheat' >> beam.Map(parse_pubsub)
        )
        
        # Tranform events
        window_toxicity = (
            parse_toxicity | 'window toxicity' >> beam.Map(preprocess_event)
                           | beam.WindowInto(window.SlidingWindows(30, 5)) # Window is 30 seconds in length, and a new window begins every five seconds
                           | beam.GroupByKey()
                           | beam.Map(avg_by_group)
        )
        
        window_griefing = (
            parse_griefing | 'window griefing' >> beam.Map(preprocess_event)
                           | beam.WindowInto(window.SlidingWindows(30, 5)) # Window is 30 seconds in length, and a new window begins every five seconds
                           | beam.GroupByKey()
                           | beam.Map(avg_by_group)
        )
        
        window_cheat = (
            parse_cheat    | 'window cheat' >> beam.Map(preprocess_event)
                           | beam.WindowInto(window.SlidingWindows(30, 5)) # Window is 30 seconds in length, and a new window begins every five seconds
                           | beam.GroupByKey()
                           | beam.Map(avg_by_group)
        )
        
        # Print results to console (for testing/debugging)
        window_toxicity | 'print toxicity' >> beam.Map(print)
        
        # ML Ensemble
        
        
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



'''
python stream_fps_logs.py --project_id gaming-demos --bq_dataset_id games --bq_table_id fps_game_logs --pubsub_topic fps_game_logs --sink pubsub --number_of_records 10 --delay 2
'''



#ZEND
