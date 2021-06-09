# Databricks notebook source
class Ingestion:

    def __init__(self, dataset_name, dw_secrets, dl_secrets):
        """
        returns a metadata driven object with attributes relating to 
        a specific dataset. Values are mainly driven out of the database or
        datawarehouse.
          
        Args:
            dataset_name ([string]): dataset name e.g sodexo / aratronspain
            dw_secrets ([dict]): [dictionary with keys and information relating to datawarehouse]
            dl_secrets ([dict]): [dictionary with keys and information relating to datalake]
        """
        self.dataset_name = dataset_name
        

        self.path_raw = Path('/').joinpath('dbfs', 'mnt', dl_secrets['root_folder'], 'raw', self.datalake_path_raw)

    def set_dataset(self):
        """ 
        returns a dataframe with metadata based on a given
        dataset."""
        sql_get_dataset = f"""(
            SELECT	ISNULL([MaxIngestionDateTime], '1 Jan 1900') AS [MaxIngestionDate]
            ,			'ext_' + [SchemaName] AS [ExternalSchemaName]
            ,			[DataLakePathRaw]
            ,			[FlatFileExtension]
            FROM		[etl_config].[IngestionDataset]
            WHERE		[DatasetName] = '{self.dataset_name}'  
            ) j
            """
        df = self.execute_sql(sql_get_dataset)
        self.max_ingestion_date = pd.to_datetime(df['MaxIngestionDate'][0])
        self.dw_external_schema_name = df['ExternalSchemaName'][0]
        self.datalake_path_raw = df['DataLakePathRaw'][0]
        self.file_extension = df['FlatFileExtension'][0]

    def set_tables(self):
        """
        Retirms a pandas dataframe with metadata regarding the given
        dataset.
        """
        sql_get_tables = f"""(
          SELECT		t.[TableName]
          ,				d.[DataLakePathRaw] + q.[DataLakePathRaw] + sp.[SchemaPath]	+ pp.[PartitionPath]	AS [DataLakePathRaw]
          ,				t.[DataLakePathCurated] + sp.[SchemaPath] + pp.[PartitionPath]						AS [DataLakePathCurated]
          ,				q.[ReadRawFileMethod]
          ,				q.[DataLakePartitionCurated]
          FROM			[etl_config].[IngestionDataset] d
          INNER JOIN	[etl_config].[IngestionTable] t
          ON			d.[DatasetKey] = t.[DatasetKey]
          INNER JOIN	[etl_config].[IngestionQuery] q
          ON			t.[TableKey] = q.[TableKey]
          CROSS APPLY 	(SELECT	'schema=v' + CAST(t.[SchemaVersion] AS varchar) + '/' AS [SchemaPath]
                      	) sp
          CROSS APPLY 	(SELECT	'partition=' + q.[DataLakePartitionCurated] + '/' AS [PartitionPath]
                      	) pp
          WHERE			d.[DatasetName] = '{self.dataset_name}'
          AND			q.[IsActive] = 1
          ) i
          """ 
    
        self.tables = self.execute_sql(sql_get_tables)

    def set_partition_schemes(self):
        """
        defines the partition scheme for the given dataset.
        """
        ps = self.tables['DataLakePartitionCurated'].unique()
        self.partition_schemes = [s for s in ps if not s == 'Latest']

    def execute_sql(self, sql_command):
        """executes a sql command and returns a pandas dataframe.
        Args:
            sql_command ([string]): SQL Code. 

        Returns:
            [pandas.dataframe]: pandas dataframe.
        """
        return sqlContext.read.jdbc(url = self.dw_url,
                                    properties = self.conn_properties,
                                    table = sql_command
                                    ).toPandas()