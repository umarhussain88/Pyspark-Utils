def read_methods():

  return {'read_csv_ricohselect' : read_csv_ricohselect, 
         'read_excel' : read_excel}


from pyspark.sql.functions import * 

def get_file_partitions(df_pyspark, col_partition):
    """[summary]

    Args:
        df_pyspark ([type]): [description]
        col_partition ([type]): [description]

    Returns:
        [type]: [description]
    """
    max_extract = (df_pyspark
                        .groupBy(col_partition, 'ingestionDate')
                        .agg(max('extractDate').alias('maxExtractDate') )
                        ) 
    max_extract_by_ingest = (max_extract
                        .groupBy(col_partition)
                        .agg(max('ingestionDate').alias('ingestionDate') )
                        )

    # join the data frames together with the derived column name
    df_partition = (max_extract_by_ingest
                    .join(max_extract
                        ,how = 'inner'
                        ,on = [col_partition, 'ingestionDate'])
                    .withColumnRenamed('ingestionDate', 'maxIngestionDate') 
                    .sort(col_partition)
                )  

    return df_partition


def get_latest_per_partition(df_pyspark,):

    df_all_data = insert_date_columns(df_pyspark)
    df_partitions = get_file_partitions(df_all_data, 
                                    col_partition = 'partitionKey')

    df_filtered = df_all_data.join(df_partitions
                                   .withColumnRenamed('maxIngestionDate','ingestionDate')
                                   .withColumnRenamed('maxExtractDate','extractDate')
                                   ,how='inner'
                                   ,on=['partitionKey', 'ingestionDate', 'extractDate']
                                  )

    # Reorder the columns so that the meta data columns are at the end & replace spaces
    df_cols_renamed = sort_columns(df_filtered)  

    # Return the filtered dataframe and the audit trail dictionary
    return df_cols_renamed


def read_csv_ricohselect(**kwargs):
  file_dataframe = kwargs['file_dataframe']
  encoding ='UTF-16' if not kwargs['encoding'] else kwargs['encoding']
  escape=None if not kwargs['escape'] else kwargs['escape']
  delim = ',' if not kwargs['delim'] else kwargs['delim']

  # convert the csv paths into a list
  file_list = file_dataframe['filePath'].apply(create_dbfs_file_path).tolist()

  # read the csv files
  return (spark
          .read
          .option('header', 'true')
          .option('multiLine', 'true')
          .option('encoding', encoding)
          .option('escape', escape)
          .option('sep',delim)
          .format('csv')
          .load(file_list)
          .withColumn('path', input_file_name())
         )