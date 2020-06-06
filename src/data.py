import pandas
import re
import os
from database_connection import databaseConnection


class dataBlob():
    """Convert various forms data of data within a single class.
    
    Input data types:
        - dataBlob
        - Pandas DataFrame
        - CSV file
        - SQL file
        - JSON file
        
    Output data types:
        - Pandas DataFrame
        - CSV
        - Database
    """
    def __init__(self, data, **kwargs):
        self.data_type = self._get_data_type(data)
        self.data = self.load_data(data)
        self._get_data_metadata()

    def load_data(self, data):
        if 'dataBlob' in str(type(data)):
            self._load_blob(data)
        elif isinstance(data, pandas.DataFrame):
            self._load_df(data)
        elif isinstance(data, str):
            if data.endswith('.csv'):
                self._load_csv_file(data)
            elif data.endswith('.json'):
                self._load_json_file(data)
            elif data.endswith('.sql'):
                self._load_sql_file(data)
            elif 'select' in data.lower() and 'from' in data.lower():
                self._load_sql(data)
            # <<< NEED TO COMPLETE DATA INPUTS HERE >>>

            # <<< NEED TO ADD EXECPTION HANDLING HERE >>>

    """
    Input data:
        Load one of several data types into the dataBlob class.
    """
    def _load_blob(self, blob):
        try:
            self.input_type = 'dataBlob'
            self.input_file = None
            self.input_code = None
            self.df = blob.df
        except Exception as e:
            print("ERROR: Unable to import dataBlob.\n{}".format(e))
    
    def _load_df(self, df):
        try:
            self.input_type = 'dataframe'
            self.input_file = None
            self.input_code = None
            self.df = df
        except Exception as e:
            print("ERROR: Unable to import DataFrame.\n{}".format(e))

    def _load_csv_file(self, csv_file):
        try:
            self.input_type = 'csv file'
            self.input_file = csv_file
            self.input_code = None
            self.df = pandas.read_csv(csv_file)
        except Exception as e:
            print("ERROR: Unable to import CSV.\n{}".format(e))

    def _load_json_file(self, json_file):
        try:
            self.input_type = 'json file'
            self.input_file = json_file
            self.input_code = None
            self.df = pandas.read_json(json_file)
        except Exception as e:
            print("ERROR: Unable to import JSON file.\n{}".format(e))

    def _load_sql_file(self, sql_file, **kwargs):
        try:
            conn = databaseConnection()
            with open(sql_file,'r') as f:
                sql = f.read()
            self.input_type = 'sql file'
            self.input_file = sql_file
            self.input_code = sql
            self.df = pandas.read_gbq(
                sql, 
                credentials=conn.database_credentials,
                **kwargs)
        except Exception as e:
            print("ERROR: Unable to import SQL file.\n{}".format(e))

    def _load_sql(self, sql, **kwargs):
        try:
            conn = databaseConnection()
            self.input_type = 'sql'
            self.input_file = None
            self.input_code = sql
            self.df = pandas.read_gbq(
                sql, 
                credentials=conn.database_credentials,
                **kwargs)
        except Exception as e:
            print("ERROR: Unable to run SQL.\n{}".format(e))


    """ OUTPUT DATA
        Output one of several data types from the dataBlob class.
    """
    def to_df(self):
        return self.df
    
    def to_json(self, **kwargs):
        return self.df.to_json(**kwargs)

    def to_csv(self, path, **kwargs):
        return self.df.to_csv(path, **kwargs)

    def to_db(self, destination_table, project_id=None, table_partition=None, table_params=None, **kwargs):
        """Output dataframe to a database destination table.
        Currently only supports BigQuery.
        """
        # Load dataframe to BigQuery via gbq()
        conn = databaseConnection()
        self.destination_table = destination_table
        self.project_id = project_id or configs.PROJECT_ID
        self.partition = table_partition
        self.params = table_params
        self.destination_table_id = self._replace_table_parameters(
            destination_table, partition, params)
        if conn.database_type=='bigquery':
            self.df.to_gbq(
                destination_table = self.destination_table_id,
                project_id=self.project_id,
                credentials=conn.database_credentials,
                **kwargs)
            # destination_dict = _get_destination_dict(destination_table)
#             self.df.to_gbq(
#                 destination_table = '{}.{}'.format(
#                     destination_dict['dataset_id'],
#                     destination_dict['table_id'],
#                 ),

    """ METADATA
        Get metadata from data object.
    """
    def _get_data_metadata(self):
        self._get_features()
        self._get_data_id()

    def _get_features(self):
        self.feature_list = list(self.df).sort()

    def _get_data_id(self):
        # Create data_id based on features included in data
        def _get_hash_id(obj):
            """Create SHA1 hash ID from any data input"""
            import hashlib
            x = str(obj).encode()
            gid = hashlib.sha1(x).hexdigest()
            return gid
        self.data_id = _get_hash_id(self.feature_list)


    """ HELPER FUNCTIONS
        Functions referenced in the code below.
    """
    def _get_data_type(self, data):
        data_type = type(data)
        return data_type
    
    def _replace_table_parameters(self, entry, partition=None, params=None):
        """Update a query or table name with today's date (i.e. YYYYMMDD)
        or custom parameters (e.g. {param_name}).
        """
        def _replace_table_partition(entry, partition=None):
            # Return table_id with YYYYMMDD notation as a date partition.
            # Date partition defaults to today if not specified.
            partition = partition or dt.datetime.now().strftime("%Y%m%d")
            return entry.replace("YYYYMMDD", partition) if "YYYYMMDD" in entry else entry

        def _replace_string(r):
            # Remove spaces or dashes in the table name
            replace = {'-': '_', ' ': '_'}
            for k, v in replace.items():
                r = r.replace(k, v)
            return r

        def _replace_table_params(entry, params):
            # Replace all parameters in the destination table name
            if isinstance(params, dict):
                for k, v in params.items():
                    entry = entry.replace('{' + k + '}', _replace_string(v))
            return entry

        # Run function
        #entry = _replace_table_partition(entry, partition)
        entry = _replace_table_params(entry, params)
        return entry