

def move_enriched_latest(pyspark_dataframe,curated_path,enriched_columns):
  pyspark_dataframe= get_latest_enriched_partition(pyspark_dataframe,enriched_columns)
  pyspark_dataframe = cast_metadatecolumns_to_string(pyspark_dataframe)
  pyspark_dataframe.write.parquet(curated_path,mode='overwrite')
  
  
def cast_metadatecolumns_to_string(pyspark_df):
    return pyspark_df.withColumn('ingestionDateTime',date_format('ingestionDateTime','yyyy-MM-dd HH:mm:SS'))\
                           .withColumn('extractDateTime',date_format('extractDateTime','yyyy-MM-dd HH:mm:SS'))


def move_json_latest(dictionary_json_dfs,curated_path,standardised_path):
  """Takes in a dictionary of json dataframes, converts to pyspark and write to parquet.
  default is to overwrite. due to polybase conversion issues and what i assume is a Pyarrow conversion
  issue, current solution is to write to CSV and let spark create the parquet file from an IO operation.
  As data is v.small not an issue. 
  """
  
  for source,table in dictionary_json_dfs.items():
    if not Path(f'/dbfs/mnt/root/tmp/json/table/').is_dir():
      Path(f'/dbfs/mnt/root/tmp/json/table/').mkdir(parents=True)
    sorted_cols = table.columns.tolist()
    sorted_cols.remove('path')
    table.to_csv(f'/dbfs/mnt/root/tmp/json/table/{source}.csv',index=False)
    sorted_cols = ['path','ingestionDateTime','extractDateTime'] + sorted_cols
    pyspark_df = spark.read.csv(f'dbfs:/mnt/root/tmp/json/table/{source}.csv',header=True,multiLine=True)
    #     pyspark_df = pandas_to_spark(table)
    pyspark_df = insert_date_columns(pyspark_df)
    pyspark_df = pyspark_df.select(*sorted_cols)

    
    pyspark_df = cast_metadatecolumns_to_string(pyspark_df)

    
    pyspark_df.write.parquet(standardised_path + '/json/' + source + '/schema=v1/partition=Latest/',
                             mode='append')
    
    pyspark_df.write.parquet(curated_path + '/json/' + source + '/schema=v1/partition=Latest/',
                             mode='overwrite')
  