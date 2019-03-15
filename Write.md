# Write SpannerIO.write
mutations = []

result = (p
		  | beam.Create(mutations)
          | SpannerIO.write(instance_id="instance",
                            database_id="database"))
# Write Mutation Groups
mutationGroups = []
result = (p
		  | beam.Create(mutationGroups)
          | SpannerIO.write(instance_id="instance",
                            database_id="database",
                            grouped=True))

class SpannerIO():
	def write(self):
		return AutoValue_SpannerIO_Write
		.setSpannerConfig(SpannerConfig.create())
		.setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES)
		.setMaxNumMUtations(DEFAULT_MAX_NUM_MUTATIONS)
		.setGroupingFactor(DEFAULT_GROUPING_FACTOR)
		.setFailureMode(FailureMode.FAIL_FAST)

SpannerConfig -> Class
	* USER_AGENT_PREFIX
	* DEFAULT_HOST
	* setProjectId
	* setInstanceId
	* setDatabaseId
	* setHost
	* setServiceFactory
	* withProjectId (call setProjectId)
	* withInstanceId (call setInstanceId)
	* withDatabaseId (call setDatabaseId)
	* withHost (call setHost)
	* withServiceFactory (call setServiceFactory)
	* connectToSpanner
	+ To create this in python, we need to set keyword arguments, because, we can't Pickle the classes.



Write -> PTransform
	* getSpannerConfig
	* getBatchSizeBytes
	* getMaxNumMUtations
	* getFailureMode
	* getScuemaReadySignal
	* getGroupingFactor
	* withSpannerConfig
	* withProjectId
	* withInstanceId
	* withDatabaseId
	* withHost
	* withServiceFactory
	* grouped
	* withFailureMode
	* withMaxNumMutations
	* withSchemaReadySignal
	* withGroupingFactor
	* expand
		(p
		 | "To mutation group" >> beam.ParDo(ToMutationGroupFn())
		 | "Write mutations to Cloud Spanner" >> WriteGrouped())
	* DisplayData
		getSpannerConfig().DisplayData
		DisplayDataItem( "batchSizeBytes", getBatchSizeBytes(), "Batch Size in Bytes")


ToMutationGroupFn -> DoFn
	* processElement
		value = c.element()
		return MutationGroup.create(value)




WriteGrouped -> PTransform
	* spec
	* BATCHABLE_MUTATIONS_TAG
	* UNBATCHABLE_MUTATIONS_TAG
	* MAIN_OUT_TAG
	* FAILED_MUTATIONS_TAG
	* CODER

	* expand
		schemaSeed = (p
					  | "Create Seed" >> beam.Create(Null))
		if spec.getSchemaReadySignal() != None:
			schemaSeed = (schemaSeed
				          | "Wait for schema" >> Wait.on(spec.getSchemaReadySignal()))

		schemaView = (schemaSeed
		              | "Read information schema" >> beam.ParDo(ReadSpannerSchema(spec.getSpannerConfig()))
		              | "Schema View" >> View)

		filteredMutations = (p
		                     | "To Global Window" >> Window.input(GlobalWindows())
		                     | "Filter Unbatchable Mutations" >> beam.ParDo(BatchableMutationFilterFn(schemaView,
		                                                                                              UNBATCHABLE_MUTATIONS_TAG,
		                                                                                              spec.getBatchSizeBytes(),
		                                                                                              spec.getMaxNumMutations())))
        batchedMutations = (filteredMutations
                            | "Gather And Sort" >> beam.ParDo(GatherBundleAndSortFn(spec.getBatchSizeBytes(),
                            													    spec.getMaxNumMutations(),
                            													    spec.getGroupingFactor(),
                            													    schemaView))
                            | "Create Batches" >> beam.ParDo(BatchFn(spec.getBatchSizeBytes(),
                                                                     spec.getMaxNumMutations(),
                                                                     schemaView), schemaView)
        result = (p
        		  | self.Merge(filteredMutations, batchedMutantions)
        		  | "Merge" >> beam.FlatMap()
        		  | "Write mutations to Spanner" >> beam.ParDo(WriteToSpannerFn(spec.getSpannerConfig(),
        		                                                                spec.getFailureMode(),
        		                                                                FAILED_MUTATIONS_TAG)))
        return SpannerWriteResult(input.getPipeline(),
                                  result.get(MAIN_OUT_TAG),
                                  result.get(FAILED_MUTATIONS_TAG),
                                  FAILED_MUTATIONS_TAG)



BatchFn
	* maxBatchSizeBytes
	* maxNumMutations
	* schemaView
	* processElement(element, scheamView)
		spannerSchema = schemaView
		batch = []
		batchSizeBytes = 0
		batchCells = 0
		for kv in element:
			mg = decode(kv.value())
			groupSize = MutationsSizeEstimator.sizeOf(mg)
			groupCells = MutationCellCounter.countof(spannerSchema, mg)
			if (batchCells+groupCells) > maxNumMutations)) or ((batchSizeBytes+groupSize) > maxBatchSizeBytes):
				c.output(batch)
				batch = []
				batchSizeBytes = 0
				batchCells = 0
			batch.add(mg)
			batchSizeBytes += groupSize
			batchCells += groupCells
		if batchCells > 0:
			c.output(batch)


BatchableMutationFilterFn
	* schemaView
	* unbatcheableMutationsTag
	* batchSizeBytes
	* maxNumMutations

	* processElement(element, schemaView)
		mg = element
		if mg.primary.getOperation() == Op.DELETE && not isPointDelete(mg.primary()):
			c.output(unbatchableMutationsTag, mg)
			return
		spannerSchema = schemaView
		groupSize = MutationSizeEstimator.sizeOf(mg)
		groupCells = MutationCellcounter.countOf(spannerschema, mg)

		if groupSize >= batchSizeBytes or groupCells >= maxNumMutations:
			c.output(unbatchableMutationsTag, mg)
		else:
			c.output(mg)

GatherBundleAndSortFn
	* maxBatchSizeBytes
	* maxNumMutations
	* batchSizeBytes
	* batchCells
	* schemaVIew
	* mutationsToSort

	* __init__(maxBatchSizeBytes, maxNumMutations, groupingFactor, schemaView)
		maxBatchSizeBytes = maxBatchSizeBytes * groupingFactor
		maxNumMutations = maxNumMutations * groupingFactor
		schemaView = schemaView

	* startBundle
		if mutationsToSort == None:
			initSorter()
		else:
			raise IllegalStateexception("Sorter should be null here")

	* initSorter
		mutationsToSort = []
		batchSizeBytes = 0
		batchCells = 0
	* finishBundle
		c.output(self.sortAndGetList(), Instant.now(), GlobalWindow.INSTANCE)

	* sortAndGetList
		mutationsToSort.sort(EncodedKvMutationGrouComparator.INSTANCE)
		tmp = mutationsToSort
		mutationsToSort = null
		return tmp
	* processElement(element, schemaView)
		spannerSchema = schemaView
		encoder = MutationKeyEncoder(spannerSchema)
		mg = element
		groupSize = MutationSizeEstimator.sizeOf(mg)
		groupCells = MutationCellCounter.countOf(spannerSchema, mg)

		syncronized(this):
			if(((batchCells + groupCells) > maxNumMutations) or (batchSizeBytes + groupSize) > maxBatchSizeBytes):
				c.output(self.sortAndGetList())
				initSorter()
			mutationsToSort.append((mg.primary(), mg))
			batchSizeBytes += groupSize
			batchCells += groupCells


WriteToSpannerFn
	* spannerAccessor
	* spannerConfig
	* failureMode
	* failedTag

	* processElement(element, )
		mutations = element
		tryIndividual = False

		try:
			batch = mutations
			spannerAccessor.getDatabaseClient().writeAtLeastOnce(batch)
			return
		except SpannerException, e:
			if failureMode == FailureMode.REPORT_FAILURES:
				tryIndividual = True
			elsif failureMode == FailureMode.FAIL_FAST:
				raise e
			else:
				raise IllegalArgumentException("Unknow failure mode " + failureMode)

		if tryIndividual:
			for mg in mutations:
				try:
					spannerAccessor.getDatabaseClient().writeAtLeastOnce(mg)
				except SpannerException, e:
					logging.warm("Failed to write the mutation group: " + mg, e)
					c.output(failedTag, mg)

SpannerWriteResult
	