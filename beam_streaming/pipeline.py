import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions
import os
from apache_beam import window

service_account = '../vida-nueva-live-f2ebe53bc837.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account

input_subscription = 'projects/vida-nueva-live/subscriptions/big-data-sub'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)
comedy_movies = 'projects/vida-nueva-live/topics/comedy_movies'

pubsub_pipeline = (
    p
    | 'Read from pubsub topic' >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | 'Split the records by comma' >> beam.Map(lambda row: row.decode("utf-8").split(','))
    | 'Filter movies with comedy genre' >> beam.Filter(lambda row: ("Comedy" in row[2]))
    | 'Converting to byte String' >> beam.Map(lambda row: (''.join(row).encode('utf-8')))
    | 'Publish to output topic' >> beam.io.WriteToPubSub(comedy_movies)
)

result = p.run()
result.wait_until_finish()
