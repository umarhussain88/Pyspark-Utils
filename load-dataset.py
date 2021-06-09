# Databricks notebook source
# MAGIC %run /Shared/ricohstore/ricohstore


dbutils.widgets.text(name='dataset', defaultValue='bidw', label='Dataset Name')
dbutils.widgets.text(name='curatedTableName', defaultValue='', label='Curated Table Name')
dataset = dbutils.widgets.get('dataset')
curated_table_name = dbutils.widgets.get('curatedTableName')
read_dict = read_methods()
partition_dict = partition_methods()


idef = Ingestion(dataset_name=dataset,
                 curated_table_name=curated_table_name)



# use this to grab data from a defined date and or datetime
# idef.max_ingestion_date = pd.Timestamp('2021-04-06 03:07:00')


# as the below function builds up paths hourly, we have to provide the path at an hourly level. i.e 2021-03-21 03:07:02 --> 2021-03-21 03:00:00 
max_ingestion_hour = pd.Timestamp(idef.max_ingestion_date.strftime('%Y-%m-%d %H:00:00'))


max_ingestion_hour.strftime('%B %d %Y')


try:
  file_dfs = pd.concat([get_max_ingestion_file_paths(max_ingestion_hour,
                                                   path,tbl) for 
     (tbl,path),data in idef.curated_table.groupby(['CuratedTableName','DataLakePathRaw'])],axis=0)
except ValueError:
  # if no files to process, gracefully exit. 
  file_dfs = pd.DataFrame({'extractDate' : pd.Timestamp('now'), 'ingestionDate' : pd.Timestamp('now')},index=[0])
  dbutils.notebook.exit(json.dumps({'max_extract_date' : file_dfs['extractDate'].max().strftime('%d %B %Y %H:%M:%S'), 'max_ingestion_date' : file_dfs['ingestionDate'].max().strftime('%d %B %Y %H:%M:%S')}))




# possible to have files on a given day but be filtered out by the boolean looking for files greater than or equal to the max ingesiton datetime,
# so no error is caught, should tidy this up as a function.

if file_dfs.shape[0] == 0:
  file_dfs = pd.DataFrame({'extractDate' : pd.Timestamp('now'), 'ingestionDate' : pd.Timestamp('now')},index=[0])
  dbutils.notebook.exit(json.dumps({'max_extract_date' : file_dfs['extractDate'].max().strftime('%d %B %Y %H:%M:%S'), 'max_ingestion_date' : file_dfs['ingestionDate'].max().strftime('%d %B %Y %H:%M:%S')}))
  


  
encoding = 'UTF-16'
if dataset == 'itrack':
  delim = '|'
else:
  delim = ','
curated_cols = idef.curated_columns.sort_values(['ColumnId'],ascending=True)

curated_column_dict = {group : val.set_index('ColumnName')['ReplaceMissingWith'].to_dict() for group, val in curated_cols.groupby('CuratedTableName')}


file_df_merged = pd.merge(file_dfs, idef.curated_table,on=['DataLakePathRaw','CuratedTableName'])
file_df_merged = file_df_merged[file_df_merged['ReadRawFileMethod'].ne('read_json_ricohselect')]


spark_dfs = {}

for (each_table,read_method,datalake_path,ext_schema,ext_table,enriched_cols,destination_db,flat_file_role,datalake_partition), data in file_df_merged.groupby(         ['CuratedTableName','ReadRawFileMethod','DataLakePath','DestinationDBExternalSchema','DestinationDBExternalTable','EnrichedBusinessKeyColumnList','DestinationDB','FlatFileRole','DataLakePartition']
                                                                                               ):
  
##   escape_char =  '"'  if each_table in ['dim_matter','formanswers','activities', 'dim_matter_udfs','requests'] else None
  spark_dfs[each_table] = read_dict[read_method](file_dataframe=data,encoding=encoding,escape='"',delim=delim)
  # temp fix for partition keys. 
  spark_dfs[each_table] = rename_columns(spark_dfs[each_table], {'Partitionkey' : 'PartitionKey'})

  selected_columns = curated_cols.loc[curated_cols['CuratedTableName'].eq(each_table)].groupby('CuratedTableName')['ColumnName'].agg('unique')[each_table].tolist()
  spark_dfs[each_table] = insert_date_columns(spark_dfs[each_table])
  
  #partition column removed from source since of long querying times -  need to revert to match configdb. 
  if each_table == 'fact_cost':
    spark_dfs[each_table] = spark_dfs[each_table].withColumn('PartitionKey', substring(col('PartitionKey'),0,6))
  
  #  handle bad column names. 
  if each_table == 'dim_matter_udfs':
    spark_dfs[each_table] = clean_columns(spark_dfs[each_table])
    
  #checks current columns and replaces missing columns with pre-set data. 
  spark_dfs[each_table] = replace_missing_columns(spark_dfs[each_table], curated_column_dict, each_table)

  spark_dfs[each_table] = spark_dfs[each_table].select(selected_columns)
  
  #check for nullType columns
  spark_dfs[each_table] = check_for_nulltype_columns(spark_dfs[each_table])
  
  #write to standardised - this is an APPEND. 
  standardised_path = idef.datalake_standardised + '/' + each_table
  partition_to_standardised(spark_dfs[each_table], 'Month',standardised_path)
  
  # clean columns and create a list - if there is a partitionkey this is added to the window function. 
  enriched_columns = clean_enriched_columns(enriched_cols,datalake_partition)

  curated_path = idef.datalake_root  + '/curated/' + datalake_path
  #first item of the list is the partition method key - YYYY/YYYYMM/YYYYMMDD default to YMD
  partition_date_cols = partition_dict['partition_dict'].get(datalake_partition)

  partition_key = datalake_partition if not partition_date_cols else partition_date_cols[0]
  
  
  # remove any and all html tags to normalize columns. 
  if each_table == 'formanswers':
    spark_dfs[each_table] = strip_html_tags(spark_dfs[each_table], ['AnswerValue','AnswerDisplayValue'])
    #extracting values using regex. 
    spark_dfs[each_table] = extract_formanswers_values(spark_dfs[each_table])
    
  if datalake_partition == 'Latest':
      move_enriched_latest( spark_dfs[each_table], curated_path,enriched_columns)
  else:
      partition_dict[partition_key](pyspark_df         =    spark_dfs[each_table],
                                    partition_column   =    'PartitionKey',
                                    flat_file_role     =    flat_file_role,
                                    date_cols          =    partition_date_cols,
                                    curated_path       =    curated_path, 
                                    enriched_columns   =    enriched_columns,
                                    datalake_partition =    datalake_partition,
                                    curated_table_name =    each_table,
                                    datasetname        =    dataset
                                        )
      
    
  # write to config db. 
  if destination_db == 'ConfigDB':
      spark_dfs[each_table] = get_latest_enriched_partition(spark_dfs[each_table], enriched_columns)
      spark_dfs[each_table].write.jdbc(url=idef.jdbc_url,properties = idef.connection_properties, 
                                                       table=f"[{ext_schema}].[{ext_table}]",
                                                       mode='overwrite')


if dataset == 'rls':
    users = spark.read.parquet('dbfs:/mnt/root/curated/Organisation/Users/schema=v1/partition=Latest/')
    users.write.jdbc(url=idef.jdbc_url,properties = idef.connection_properties, 
                                                         table=f"[{ext_schema}].['users']",
                                                         mode='overwrite')




dbutils.notebook.exit(json.dumps({'max_extract_date' : file_dfs['extractDate'].max().strftime('%d %B %Y %H:%M:%S'), 'max_ingestion_date' : file_dfs['ingestionDate'].max().strftime('%d %B %Y %H:%M:%S')}))


