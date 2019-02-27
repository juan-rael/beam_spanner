



# class _SpannerWriteFn(beam.DoFn):
#   def __init__(self, project_id, instance_id, table_id,
#                flush_count=FLUSH_COUNT,
#                max_row_bytes=MAX_ROW_BYTES):
#       pass
#   def __getstate__(self):
#     return None

#       def __setstate__(self, options):
#       pass
#   def start_bundle(self):
#       pass
#   def process(self, element):
#       pass
#   def finish_bundle(self):
#       pass
#   def display_data(self):
#     return {'projectId': DisplayDataItem(self.beam_options['project_id'],
#                                          label='Bigtable Project Id'),
#             'instanceId': DisplayDataItem(self.beam_options['instance_id'],
#                                           label='Bigtable Instance Id'),
#             'tableId': DisplayDataItem(self.beam_options['table_id'],
#                                        label='Bigtable Table Id')
#            }
# class WriteToBigTable(beam.PTransform):
#   """ A transform to write to the Bigtable Table.

#   A PTransform that write a list of `DirectRow` into the Bigtable Table

#   """
#   def __init__(self, ):
#     super(WriteToBigTable, self).__init__()

#   def expand(self, pvalue):
#     return (pvalue
#             | beam.ParDo(_SpannerWriteFn(beam_options['project_id'],
#                                          beam_options['instance_id'],
#                                          beam_options['table_id'],
#                                          beam_options['flush_count'],
#                                          beam_options['max_row_bytes'])))