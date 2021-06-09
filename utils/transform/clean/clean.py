# Databricks notebook source
from pyspark.sql.functions import *
import re 
import pandas as pd
from pyspark.sql.types import DoubleType, NullType


def insert_date_columns(pyspark_dataframe, column_path='path',
                        regex_IngestionDate = 'year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/hour=(\d{2})\/minute=(\d{2})',
                        regex_ExtractDate = '(\d{8}-\d{6})',
                        extract_format='yyyyMMdd-HHmmss'):
    """[summary]

    Args:
        pyspark_dataframe ([type]): [description]
        column_path (str, optional): [description]. Defaults to 'path'.
        regex_IngestionDate (str, optional): [description]. Defaults to 'year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/hour=(\d{2})\/minute=(\d{2})'.
        regex_ExtractDate (str, optional): [description]. Defaults to '(\d{8}-\d{6})'.
        extract_format (str, optional): [description]. Defaults to 'yyyyMMdd-HHmmss'.

    Returns:
        [type]: [description]
    """
    
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    pat = re.compile(regex_IngestionDate)
    date_format_dict = {'year' : 'yyyy', 'month' : 'MM', 'day' : 'dd', 'hour'  : 'HH', 'minute' : 'mm','second' : 'ss'}
    date_format_matches =  re.findall(r'year|month|day|hour|minute|second',regex_IngestionDate)
    
    

    ingestion_format = ''.join([format_val for key,format_val in date_format_dict.items() if key in date_format_matches])
    
    ingestionDate = concat(*[regexp_extract(col(column_path),regex_IngestionDate,i) for i in range(1,pat.groups + 1)])

    
    ExtractDate = coalesce(to_timestamp(regexp_extract
                             (col(column_path), regex_ExtractDate, 1), extract_format),
                              to_timestamp(ingestionDate, ingestion_format)
                              )
    return (pyspark_dataframe.withColumn('ingestionDateTime', 
                              to_timestamp(ingestionDate, ingestion_format))
                              .withColumn('extractDateTime',ExtractDate))




def rename_columns(df, columns : dict):
    ''' 
    similair function to pandas 
    dataframe.rename(columns={'src' : 'trg'})
    takes in two arguments, the columns (dictionary)
    and the dataframe.

    '''
    if isinstance(columns, dict):
        for old_name, new_name in columns.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")


# Reorder and rename columns in a Pyspark dataframe
def sort_columns(df_pyspark):
    ''' makes of two libraries from the python standard lib
    re : regular expression replacement
    string : string module - will use the string.punctuation
    to relace all punctuation marks.
    '''
    # initiate our custom translator from the string module.
    # the first two arguments are non consquential, however the third will be replaced with None.
    # only works with ASCII characaters. 
    translator = str.maketrans('','','!"#$%&\'*+,./:;<=>@[\\]^`{|}~\\r') 
    # Reorder the columns so that the meta data columns are at the end & replace spaces
    cols_meta = ['path', 'ingestionDateTime', 'extractDateTime']
    cols_order = [c for c in df_pyspark.columns if not c in cols_meta] + cols_meta
    #remove all punctuation marks 
    cols_punctuation_removed = [col.strip().translate(translator) for col in cols_order] 
    cols_rename = [re.sub(' ','_',col) for col in cols_punctuation_removed]

    df_renamed = rename_columns(df_pyspark,dict(zip(cols_order,cols_rename)))


    # Return the dataframe with new column order and cleansed names
    return df_renamed
  
  
  
def create_data_lake_posix_path(file_path):
    fp = Path(file_path)
    pat = re.compile(r'raw|curated',flags=re.IGNORECASE)
    if not fp.is_dir():
        p = fp.parts
        if p[0] == 'dbfs:':
            return Path('/dbfs/').joinpath(*p[1:])
        else:
            start_p = re.search(pat,'|'.join(p)).group(0)
            idx = p.index(start_p)
            return Path('/dbfs/mnt/root').joinpath(*p[idx:])
    else:
        return fp
  
def create_dbfs_file_path(file_path):

    if isinstance(file_path,str):
        if file_path.split('/')[0] != 'dbfs:':
            return 'dbfs:/' + file_path
        if dbutils.fs.ls(file_path):
            return file_path
    else:
      fp = create_data_lake_posix_path(file_path)
      return 'dbfs:/' + '/'.join(fp.parts[2:])


        


# mainly form answers data.
def strip_html_tags(pyspark_dataframe, columns : list):
  for column in columns:
    pyspark_dataframe = pyspark_dataframe.withColumn(column, regexp_replace(column, r'<.*?>',''))
    pyspark_dataframe = pyspark_dataframe.withColumn(column, regexp_replace(column, r'&nbsp',' '))
    pyspark_dataframe = pyspark_dataframe.withColumn(column, trim(column))
  return pyspark_dataframe
    
def extract_formanswers_values(pyspark_df):

  trg_answer_col = 'AnswerIntegerValue'

  user_id_questions = ['H_SupervisingFE','H_BillingFE','H_ClientPartner']
  pyspark_df = pyspark_df.withColumn(trg_answer_col,
                                    when(col('QuestionName').isin(user_id_questions), 
                                         regexp_extract(col('AnswerValue'), '(\D+)\s(\d+)$',2)
                                        ).otherwise(None)
                                    )
  
  #get assoicate matter_id
  pyspark_df = pyspark_df.withColumn(trg_answer_col, when((col('QuestionName')=='AM_AssocMatters') & (col('AnswerSearchValue') != 'null') , 
                                                               regexp_extract(col('AnswerValue'), '(\d+\.\d+)',1)
                                                                             ).otherwise(col(trg_answer_col)
                                                               )
                                    )
  
  pyspark_df = pyspark_df.withColumn(trg_answer_col, when((col('QuestionName').contains('WorkType')),
                                                                                 regexp_extract(col('QuestionName'),'MT_WorkType_(.*)',1)).otherwise(col(trg_answer_col))
                                    )
  
  pyspark_df = pyspark_df.withColumn(trg_answer_col, when(col('QuestionName').contains('WorkType'), 
                                     regexp_replace(col(trg_answer_col),'_', ' ' )).otherwise(col(trg_answer_col)) 
                                    )
  
  pyspark_df = pyspark_df.withColumn(trg_answer_col, when(col('QuestionName').contains('WorkType'), 
                                     regexp_replace(col(trg_answer_col), "(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])" , r" ")).otherwise(col(trg_answer_col)) 
                                    )
  
  return pyspark_df


def clean_columns(df_pyspark):
    ''' makes of two libraries from the python standard lib
    re : regular expression replacement
    string : string module - will use the string.punctuation
    to relace all punctuation marks.
    '''
    # initiate our custom translator from the string module.
    # the first two arguments are non consquential, however the third will be replaced with None.
    # only works with ASCII characaters. 
    translator = str.maketrans('','','!"#$%&\'*+,.:;<=>@[\\]^`{|}~\\') 
    # Reorder the columns so that the meta data columns are at the end & replace spaces
    #remove all punctuation marks 
    cols_punctuation_removed = [col.strip().translate(translator).strip() for col in df_pyspark.columns] 
    cols_rename = [re.sub(' ','_',col) for col in cols_punctuation_removed]

    df_renamed = rename_columns(df_pyspark,dict(zip(df_pyspark.columns,cols_rename)))

    # Return the dataframe with new column order and cleansed names
    return df_renamed


def check_for_nulltype_columns(pyspark_dataframe):
    """Checks for any nulltype columns, as these aren't supported in Spark/Parquet yet.
    returns a dataframe with the columns casted to DoubleType()
    """
    schema = pyspark_dataframe.schema
    null_columns = [column.name for column in schema if column.dataType == NullType()]
    
    if null_columns:
      for null_col in null_columns:
          pyspark_dataframe = pyspark_dataframe.withColumn(null_col, col(null_col).cast(DoubleType())) 

    return pyspark_dataframe
    


def replace_missing_columns(pyspark_dataframe, curated_column_dict,table_name):
    current_columns = pyspark_dataframe.columns
    missing_columns = list(set(curated_column_dict[table_name].keys()) - set(current_columns))
    
    for missing_column in missing_columns:
        
        pyspark_dataframe = pyspark_dataframe.withColumn(missing_column, lit(curated_column_dict[table_name][missing_column]))
    return pyspark_dataframe
    


def clean_enriched_columns(enriched_df : str, partition_key : str) -> list:
  partition_col = ['PartitionKey'] if partition_key in ['YYYY', 'YYYYMM', 'YYYYMMDD'] else []
  cols =  [i.strip() for i in re.sub(r'\[|\]', '',enriched_df).split(',')]
  return [] if cols == [''] else cols + partition_col


def get_furthest_relative_path(posix_path):

  paths = {}
  for dir_ in posix_path.rglob('*'):
    p = (dir_.relative_to(posix_path).parts)
    if dir_.is_dir():
      paths[dir_] = len(p)
  max_p = sorted(paths,key=lambda x : x in paths.values())[-1]
  
  return create_dbfs_file_path(max_p)
      