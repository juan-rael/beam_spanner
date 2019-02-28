from __future__ import absolute_import
import argparse
import datetime
import uuid


import apache_beam as beam
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.spanner import Client
from google.cloud.spanner_v1.session import Session



EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
label_stamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
label_stamp_micros = _microseconds_from_datetime(label_stamp)
LABELS = {LABEL_KEY: str(label_stamp_micros)}


class GenerateRow(beam.DoFn):
  def __init__(self):
    from apache_beam.metrics import Metrics
    self.generate_row = Metrics.counter(self.__class__, 'generate_row')

  def __setstate__(self, options):
    from apache_beam.metrics import Metrics
    self.generate_row = Metrics.counter(self.__class__, 'generate_row')

  def process(self, ranges):
    from faker import Faker
    fake = Faker()
    for row_id in range(int(ranges[0]), int(ranges[1][0])):
      self.generate_row.inc() 
      yield (row_id, fake.name())


class CreateAll():
  def __init__(self, project_id, instance_id, database_id, table_id):
    self.project_id = project_id
    self.instance_id = instance_id
    self.database_id = database_id
    self.table_id = table_id
    self.client = Client(project=self.project_id)

  def create_table(self):
    instance = self.client.instance(self.instance_id)
    database = instance.database(self.database_id)
    if not database.exists():
      database = instance.database(self.database_id, ddl_statements=[
          """CREATE TABLE """+self.table_id+""" (
              keyId     INT64 NOT NULL,
              Name    STRING(1024),
          ) PRIMARY KEY (keyId)""",])

      operation = database.create()
      operation.result()
      print('Database and Table Created')


class SpannerWriteFn(beam.DoFn):
  def __init__(self, project_id, instance_id,
               database_id, table_id, columns,
               max_num_mutations=10000,
               batch_size_bytes=0):
    from google.cloud.spanner import Client
    from apache_beam.metrics import Metrics

    super(SpannerWriteFn, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'database_id': database_id,
                         'table_id': table_id,
                         'columns': columns,
                         'max_num_mutations': max_num_mutations,
                         'batch_size_bytes': batch_size_bytes}
    client = Client(project=self.beam_options['project_id'])
    instance = client.instance(self.beam_options['instance_id'])
    database = instance.database(self.beam_options['database_id'])
    # Create a Session
    self.session = Session(database)
    self.session.create()

    self.written_row = Metrics.counter(self.__class__, 'Written Row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    from google.cloud.spanner import Client
    from google.cloud.spanner_v1.session import Session
    from apache_beam.metrics import Metrics

    self.beam_options = options
    client = Client(project=self.beam_options['project_id'])
    instance = client.instance(self.beam_options['instance_id'])
    database = instance.database(self.beam_options['database_id'])
    # Create a Session
    self.session = Session(database)
    self.session.create()

    self.written_row = Metrics.counter(self.__class__, 'Written Row')

  def start_bundle(self):
    self.transaction = self.session.transaction()
    self.transaction.begin()
    self.values = []

  def _insert(self):
    self.transaction.insert(
          table=self.beam_options['table_id'],
          columns=self.beam_options['columns'],
          values=self.values)
    self.transaction.commit()
    self.written_row.inc(len(self.values))

  def process(self, element):
    if len(self.values) >= self.beam_options['max_num_mutations']:
      
      self._insert()
      self.transaction = self.session.transaction()
      self.transaction.begin()
      self.values = []
    else:
      self.values.append(element)
      

  def finish_bundle(self):
    if len(self.values) > 0:
      self._insert()      
    self.transaction = None
    self.values = []

  def display_data(self):
    return {
      'projectId': DisplayDataItem(self.beam_options['project_id'],
                                   label='Spanner Project Id'),
      'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                    label='Spanner Instance Id'),
      'databaseId': DisplayDataItem(self.beam_options['database_id'],
                                    label='Spanner Database Id'),
      'tableId': DisplayDataItem(self.beam_options['table_id'],
                                 label='Spanner Table Id'),
    }
def run(argv=[]):
  project_id = 'grass-clump-479'
  instance_id = 'python-write'
  guid = str(uuid.uuid4())[:8]
  database_id = 'pythontest'+ guid
  guid = str(uuid.uuid4())[:8]
  table_id = 'pythontable'
  jobname = 'spanner-write-' + guid
  

  argv.extend([
    '--experiments=beam_fn_api',
    '--project={}'.format(project_id),
    '--instance={}'.format(instance_id),
    '--job_name={}'.format(jobname),
    '--requirements_file=requirements.txt',
    '--disk_size_gb=50',
    '--region=us-central1',
    '--runner=dataflow',
    '--autoscaling_algorithm=NONE',
    '--num_workers=5',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
  ])
  parser = argparse.ArgumentParser(argv)
  (known_args, pipeline_args) = parser.parse_known_args(argv)

  create_table = CreateAll(project_id, instance_id, database_id, table_id)
  print('ProjectID:',project_id)
  print('InstanceID:',instance_id)
  print('DatabaseID:',database_id)
  print('TableID:',table_id)
  print('JobID:', jobname)
  create_table.create_table()

  row_count = 10000000
  row_limit = 1000
  row_step = row_count if row_count <= row_limit else row_count/row_limit
  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p = beam.Pipeline(options=pipeline_options)
  
  count = (p
           | 'Ranges' >> beam.Create([(str(i),str(i+row_step)) for i in xrange(0, row_count, row_step)])
           | 'Group' >> beam.GroupByKey()
           | 'Generate' >> beam.ParDo(GenerateRow())
           | 'Print' >> beam.ParDo(SpannerWriteFn(project_id,
                                                  instance_id,
                                                  database_id,
                                                  table_id,
                                                  columns=('keyId', 'Name',))))
  p.run()

if __name__ == '__main__':
  run()
