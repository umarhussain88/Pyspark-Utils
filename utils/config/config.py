# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import *
import numpy as np
from pathlib import Path

# Although this works - it is slow as it uses a recursive method to find the file paths. 
def get_latest_files(raw_path, max_ingestion_date, file_extension='*'):
    """Returns a list of the latest files after a certain date.
       the date in the filename is used to ascertain the date.

    Args:
        raw_path ([str]): location of mounted gen2 datalake with raw files.
        max_ingestion_date ([datetime]): datetime of latest successful
                                         pipeline run.
        file_extension ([string]): optional, looks for files of a default type.
    """
    pat = r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})"
    
    rp = create_data_lake_posix_path(raw_path)
    
    files = {file.stem : [file.resolve(), file.stat().st_size]
                for file in rp.rglob(f"*.{file_extension}")}

    df_files = (
        pd.DataFrame.from_dict(files, orient="index",
                               columns=["filePathRaw", "fileSize"])
        .rename_axis("fileName")
        .reset_index()
    )
    df_files['filePath'] = df_files['filePathRaw'].astype(str)\
                                                  .str.strip('/dbfs')
    df_files['ingestionDate'] = pd.to_datetime(df_files['filePath']\
                                .str.extractall(pat).agg('-'.join,1)\
                                .reset_index(1,drop=True),
                                format='%Y-%m-%d-%H-%M')

    df_files['extractDate'] = pd.to_datetime(df_files['filePath']\
                                             .str.extract(r'(\d{8}-\d{6})')[0],
                                             format = '%Y%m%d-%H%M%S')    
    df_files["extractDate"] = np.where(
        df_files["extractDate"].isnull(),df_files["ingestionDate"],
                                         df_files["extractDate"])
    df_files['DataLakePathRaw'] = raw_path



    return df_files.loc[(df_files['ingestionDate'] > max_ingestion_date) 
                          & (df_files['fileSize'] > 0), ['fileName', 'filePath','DataLakePathRaw', 'ingestionDate','extractDate']]


def get_max_ingestion_file_paths(max_ingestion_date : pd.Timestamp, raw_path : Path, table ):
    
    """Returns file paths at hourly level.
    uses the delta from today and the max ingestion date to infer all the possible hourly paths.
    we then test to see if they are true and return the true minute level file paths as a list.
    """
    pat = r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/minute=(\d{2})"

    delta_days = pd.date_range(idef.max_ingestion_date,pd.Timestamp('now') + pd.DateOffset(days=1),freq='H')
    minute_file_paths = []
    for each_hour in delta_days:
        dt = each_hour 
        mnth = f"{dt.month}".rjust(2,'0')
        h = f"{dt.hour}".rjust(2,'0')
        day = f"{dt.day}".rjust(2,'0')
        p = create_data_lake_posix_path(raw_path).joinpath(f'year={dt.year}',f'month={mnth}',f'day={day}',f'hour={h}')
        if p.is_dir():
          for each_path in p.iterdir():
              minute_file_paths.append(each_path)
              
    file_dict = {p : list(p.glob('*')) for p in minute_file_paths}
    if file_dict:

      df_files = pd.DataFrame({'path' : k, 'filePathPosix' : v } for k,v in file_dict.items()).explode('filePathPosix')


      df_files['filePath'] = df_files['filePathPosix'].astype(str)\
                                                  .str.strip('/dbfs')

      df_files['ingestionDate'] = pd.to_datetime(df_files['filePath']\
                                  .str.extractall(pat).agg('-'.join,1)\
                                  .reset_index(1,drop=True),
                                  format='%Y-%m-%d-%H-%M')

      df_files['extractDate'] = pd.to_datetime(df_files['filePath']\
                                                  .str.extract(r'(\d{8}-\d{6})')[0],
                                                  format = '%Y%m%d-%H%M%S')    
      df_files["extractDate"] = np.where(
          df_files["extractDate"].isnull(),df_files["ingestionDate"],
                                      df_files["extractDate"])
      df_files['DataLakePathRaw'] = str(raw_path)
      df_files['CuratedTableName'] = table

      return df_files.loc[(df_files['ingestionDate'] > max_ingestion_date)][['filePath','DataLakePathRaw', 'ingestionDate','extractDate','CuratedTableName']]
        
        



def execute_sql(query):
    jdbc_url,connection_properties=get_jdbc_url()
    return spark.read.jdbc(url=jdbc_url,table=query,properties=connection_properties).toPandas()



class Ingestion:

   
    def __init__(self, dataset_name,
                 curated_table_name,
                 config_flag=False):
        """ 
           returns a metadata driven object with attributes relating to 
           a specific dataset. Values are mainly driven out of the database or
           datawarehouse.

        Args:
            dataset_name ([string]): dataset name
            curated_table_name([table_name]): table name, if '' then returns all tables.

        Returns:
            [class object]: object with various methods and attributes.
        """
  

        self.dataset_name = dataset_name
        self.jdbc_url,self.connection_properties = get_jdbc_url()
        self.config_flag = config_flag
        self.curated_table_name = curated_table_name
        

        
        datalake_root_qry = "(SELECT DataLakeRootFolder FROM etl_config.Configuration) datalake_root"
        datalake_root = execute_sql(query=datalake_root_qry)
        self.datalake_root = create_dbfs_file_path('dbfs:/mnt/' + datalake_root['DataLakeRootFolder'][0])

        config_qry = f"""(SELECT MaxIngestionDateTime, FlatFileExtension, SchemaName FROM [etl_config].[Dataset] WHERE
                        DatasetName = '{self.dataset_name}') config"""
        config_df = execute_sql(query=config_qry)

        curated_view = f"""(SELECT * FROM [etl_config].[v_CuratedTable]  WHERE DatasetName = '{self.dataset_name}' 
                            AND (CuratedTableName = '{self.curated_table_name}' OR '{self.curated_table_name}' = '')
                            ) etl"""

        self.curated_table = execute_sql(query=curated_view)


        json_view = f"(SELECT * FROM [etl_config].[v_JsonTable]  WHERE DatasetName = '{dataset_name}') etl"
        self.json_tables = execute_sql(query=json_view)



        curated_columns_qry = f"""(SELECT * FROM [etl_config].[v_CuratedColumn] WHERE DatasetName = '{self.dataset_name}'
                                AND (CuratedTableName = '{self.curated_table_name}' OR '{self.curated_table_name}' = '')
                                ) curated_cols"""
        self.curated_columns = execute_sql(query=curated_columns_qry)


        self.max_ingestion_date =  config_df['MaxIngestionDateTime'][0]
        self.schema_name =  config_df['SchemaName'][0]
        self.file_extension =  config_df['FlatFileExtension'][0]
        self.datalake_standardised = self.datalake_root + '/standardised'
        self.datalake_curated = self.datalake_root + '/curated'

        



def tree(directory,dir_only=False):
    print(f'+ {directory}')
    for path in sorted(directory.rglob('*')):
        depth = len(path.relative_to(directory).parts)
        spacer = '    ' * depth
        if dir_only:
          if path.is_dir():
            print(f'{spacer}+ {path.name}')
        else:
          print(f'{spacer}+ {path.name}')
          
        
def rm_recursive(pth):
    pth = Path(pth)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_recursive(child)
    pth.rmdir()        