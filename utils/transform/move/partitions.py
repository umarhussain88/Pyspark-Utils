def partition_legacy(**kwargs):
        # this returns a list of columns that are used in partitioning a dataframe .i.e Year, Month, DayofMonth/Year
        # YMD is a meta field found in every partition that has a yyyy - yyyymm - or yyyymmdd partition type.
        curated             = None if not kwargs['curated_path']        else  kwargs['curated_path']
        flat_file_role      = None if not kwargs['flat_file_role']      else  kwargs['flat_file_role']
        datalake_partition  = None if not kwargs['datalake_partition']  else  kwargs['datalake_partition'] 
        enriched_columns    = list(kwargs['enriched_columns']) 
        legacy_df           = kwargs['pyspark_df']
        
        if Path(
                create_data_lake_posix_path(curated_path)
                ).is_dir():
                curated_df = spark.read.parquet(curated_path)
                legacy_df = curated_df.unionAll(legacy_df)

        
        legacy_df = get_latest_enriched_partition(legacy_df,enriched_columns + ['question_name','question_value']) 
        
        legacy_df = cast_metadatecolumns_to_string(legacy_df) 

        legacy_df.write.parquet(curated,mode='overwrite')


from collections import OrderedDict

# spark.conf.set('spark.sql.sources.partitionOverwriteMode','overwrite')


def timecard_partitions():
  """Repartitions Timecard DF into YYYYMM for polybase."""
  timecard_df = spark.read.parquet('dbfs:/root/curated/Activity/Timecard/schema=v1/partition=YYYYMMDD/')
  timecard_df = timecard.withColumn('PartitionKey', col('PartitionKey'), )


def get_latest_enriched_partition(pyspark_df,enriched_columns):
  """
  Returns the latest data based on key business columns ordered by max 
  extract and ingestion dates.
  

  Args:
      pyspark_df ([type]): [description]
      enriched_columns ([type]): [description]

  Returns:
      [type]: [description]
  """
  if not enriched_columns:
      enriched_columns = pyspark_df.columns
      enriched_columns = [col for col in enriched_columns if col not in ['path', 'ingestionDateTime','extractDateTime']]
    
  return pyspark_df.withColumn("row_number", row_number().over(Window.partitionBy(*enriched_columns)
                                                     .orderBy(col('ingestionDateTime').desc(),
                                                              col('extractDateTime').desc()))
                ).filter(col('row_number') == 1).drop('row_number')


def write_updated_partitions_to_configdb(pyspark_df,date_cols,curated_table_name,datasetname):
    """ 
    Writes partitions paths to config db, we will use these to dynamically rebuild the larger dim/fact tables to increase performance.
    columns currently written
    PartitionPath    - Year=2020/Month=11  
    Path             - First instance of Path if debugging is needed at a file level. 
    MaxExtractDate   - For deduping data.
    MaxIngestionDate - For deduping data.
    CuratedTableName - To join onto etl_config.curatedtables.
    DatasetName      - To join onto etl_config.DatasetName
    Mode is currently set to append best to let the db handle deduping and truncation. 

    """
    filtered_df = pyspark_df.groupBy(*date_cols)\
                .agg(
                    first('path').alias('path'), 
                    max('extractDateTime').alias('maxExtractDate'), 
                    max('ingestionDateTime').alias('maxIngestionDateTime') 
                    ).orderBy(*date_cols)

    pandas_df = filtered_df.withColumn('CuratedTableName',lit(curated_table_name)).toPandas()

    #since these datasets will be small it will be easier to manipulate in pandas. 
    pyspark_df = pandas_to_spark(
      # join along axis.
      pandas_df.join(
        pandas_df[date_cols].stack()\
       .reset_index(1)\
       .astype(str)\
       .agg('='.join,1)\
       .groupby(level=0).agg('/'.join).to_frame('PartitionLocation'))\
       .drop(date_cols,1))


    pyspark_df.write.jdbc(url=idef.jdbc_url,
                          properties = idef.connection_properties, 
                          table='[ext_config].[PartitionColumnUpdates]',
                          mode='append')




def union_overwrite_recent_partition(pyspark_df,date_cols,curated_table_name,curated_path,enriched_columns,datasetname):
    """
    Takes in a 'recent' table i.e a delta and does the following
    1. A left semi join on the curated table to return matching keys on the left table only. 
    2. A UnionAll to join the dataframe.
    3. de dupes based on the Partition Columns & Business Columns. for Fact_Timecard this is Year, Month & timecard_id
    4. calls the the write_update_partitions_to_config_db function
    5. re-writes partition to lake as 'overwrite.dynamic' - reset to static to stop any commit errors after job is complete.
    """
   #remove YMD meta item from list.
    enriched_columns.remove('PartitionKey') if 'PartitionKey' in enriched_columns else enriched_columns
    
    # update config db with partitions from this dataset, returns a dict to build up method call. 
#     write_updated_partitions_to_configdb(pyspark_df,date_cols,curated_table_name,datasetname)
    
    # get curated file and read in keys that exist in right dataframe, although this says `join` no join occurs. 
    semi_df = (spark
                    .read
                    .parquet(curated_path
                                       ).join(
                                        pyspark_df,on=date_cols,
                                        how='left_semi'))
    
    #union delta and curated tables. 
    semi_df = pyspark_df.select(*semi_df.columns).unionAll(semi_df)
    # de dupe at each partition level by the business key. 
    latest_partitions = get_latest_enriched_partition(
                                  pyspark_df       = semi_df,
                                  enriched_columns = enriched_columns + date_cols )
    
    
    
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    #write latest to lake at relevant partition level.
    latest_partitions = cast_metadatecolumns_to_string(latest_partitions)
    latest_partitions.write.parquet(curated_path,partitionBy=date_cols,mode='overwrite')
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','static')

    

   
  


def create_partition_columns(pyspark_df,date_cols,partition_column,datalake_partition):
        pyspark_date_methods = {'Year' : year, 'Day' : dayofmonth, 'Month' : month}
        partition_formats = {'YYYYMM' : 'yyyyMM', 'YYYY' : 'yyyy', 'YYYYMMDD' : 'yyyyMMdd'}
        date_format = partition_formats.get(datalake_partition)
      
        methods_to_use = {partition : method for partition,method in pyspark_date_methods.items() 
                                                                  if partition in date_cols }
    
        partition_df =  pyspark_df.withColumn('PartitionDate', 
                                               to_date(col(partition_column),
                                               format=date_format))

        # add in the relevant date partition_columns Year = 2020, Month = 12 etc.
        for partition, method in methods_to_use.items():
            partition_df = partition_df.withColumn(partition, method('PartitionDate').cast('string'))
        return partition_df 


def partition_to_standardised(pyspark_dataframe, partition_level,path):

    partition_dict = OrderedDict([('Year', year),
    ('Month', month),
    ('Week', weekofyear),
    ('day_of_year',dayofyear)])

    if partition_level not in partition_dict.keys():
      raise KeyError(f"Your Key must be one of the following {', '.join(partition_dict.keys())}")

    key_col_index = list(partition_dict.keys()).index(partition_level)
    partition_cols = {k : v for num,(k,v) in enumerate(partition_dict.items()) if num <= key_col_index}

    for column, function in partition_cols.items():
      pyspark_dataframe = pyspark_dataframe.withColumn(column, function('ingestionDateTime'))
    pyspark_dataframe.write.mode('append').partitionBy(*partition_cols.keys()).parquet(path)


def partition_methods():
  

    def partition_by_string(**kwargs):
      #constant variables.
      date_cols            = [] if not kwargs['date_cols'] else list(kwargs['date_cols'])
      kwargs['pyspark_df'] =  rename_columns(kwargs['pyspark_df'],{kwargs['partition_column'] : 'Part'})
      kwargs['pyspark_df'] = cast_metadatecolumns_to_string(kwargs['pyspark_df'])
      kwargs['pyspark_df'] = kwargs['pyspark_df'].drop('PartitionDate')
      flat_file_role      = None if not kwargs['flat_file_role']      else  kwargs['flat_file_role']
      datalake_partition  = None if not kwargs['datalake_partition']  else  kwargs['datalake_partition'] 
      curated             = None if not kwargs['curated_path']        else  kwargs['curated_path']

      if flat_file_role in ['History','Full']:

        spark.conf.set('spark.sql.sources.partitionOverwriteMode','static')
        partition_df = get_latest_enriched_partition(kwargs['pyspark_df'],kwargs['enriched_columns']) 
        partition_df.repartition(1).write.parquet(kwargs['curated_path'],partitionBy='Part',mode='overwrite')

        # recent.
      if flat_file_role == 'Recent':
        union_overwrite_recent_partition(pyspark_df            = kwargs['pyspark_df'],
                                        date_cols             = ['Part'], # these are the columns to join on CHANGE variable name from date_cols.
                                        curated_table_name    = kwargs['curated_table_name'],
                                        curated_path          = curated,
                                        enriched_columns      = enriched_columns, 
                                        datasetname           = kwargs['datasetname']
                                        )
            

    def partition_by_str_yyyymmdd(**kwargs):

        # this returns a list of columns that are used in partitioning a dataframe .i.e Year, Month, DayofMonth/Year
        # YMD is a meta field found in every partition that has a yyyy - yyyymm - or yyyymmdd partition type.
        date_columns        = [column for column in kwargs['date_cols'] if column != 'YMD']        
        curated             = None if not kwargs['curated_path']        else  kwargs['curated_path']
        flat_file_role      = None if not kwargs['flat_file_role']      else  kwargs['flat_file_role']
        datalake_partition  = None if not kwargs['datalake_partition']  else  kwargs['datalake_partition'] 
        enriched_columns    = list(kwargs['enriched_columns']) 
        
        #returns YMD partition cols.
        partition_df = create_partition_columns(kwargs['pyspark_df'],
                                 date_columns,
                                 kwargs['partition_column'],
                                 datalake_partition)


        if flat_file_role in ['History','Full']:

            spark.conf.set('spark.sql.sources.partitionOverwriteMode','static')
            partition_df = get_latest_enriched_partition(partition_df,kwargs['enriched_columns']) 
            partition_df = partition_df.drop(*['PartitionKey','PartitionDate'])
            partition_df = cast_metadatecolumns_to_string(partition_df)
            partition_df.repartition(1).write.parquet(kwargs['curated_path'],partitionBy=date_columns,mode='overwrite')
            
        # recent.
        if flat_file_role == 'Recent':
          union_overwrite_recent_partition(pyspark_df            = partition_df,
                                           date_cols             = date_columns,
                                           curated_table_name    = kwargs['curated_table_name'],
                                           curated_path          = curated,
                                           enriched_columns      = enriched_columns, 
                                           datasetname           = kwargs['datasetname']
                                          )

    # first level of dict is used to filter out the keys. 
    return { 'partition_dict' : {'YYYY'     : ['YMD','Year'], 
                                 'YYYYMM'   : ['YMD','Year','Month'], 
                                 'YYYYMMDD' : ['YMD','Year','Month','Day']},
            
            'latest_enriched_partition' : get_latest_enriched_partition,
            'YMD'                       : partition_by_str_yyyymmdd,
            'String'                    : partition_by_string,
             'Legacy'                   : partition_legacy}


