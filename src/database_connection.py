#!/usr/bin/env python3

import os
import configs


class databaseConnection():

    def __init__(
        self,
        database_type='bigquery',
        credentials_filepath=None
    ):
        self.supported_databases = ['bigquery']
        self.credentials_filepath = credentials_filepath or \
            configs.CREDENTIALS_FILEPATH
        self.configs_filepath = configs.CONFIGS_FILEPATH
        self.database_type = self._check_database_type(database_type)
        self.credentials = self.update_database_credentials()

    def _check_database_type(self, database_type):
        if database_type.lower() in self.supported_databases:
            return database_type.lower()
        else:
            print("""ERROR: This database type is not supported.
            See self.supported_databases for a complete list.""")
            return None

    def update_database_credentials(self):
        if os.path.exists(self.credentials_filepath):
            if self.database_type == 'bigquery':
                self.database_credentials = self._get_db_bigquery_credentials()
            # elif: <<< add other database credentials types here >>>
        else:
            self._get_credentials_filepath()
            self.database_credentials = self._get_db_bigquery_credentials()
        return self.database_credentials

    def _get_credentials_filepath(self):
        """
        Check if creds file is available; otherwise
        prompt for Service Key upload
        """
        credentials_filepath = input(
            "Enter the file path and name for "
            "GCP's Service Account credentials file. See for more details:\n"
            "https://console.cloud.google.com/iam-admin/"
            "serviceaccounts?project=<projectId>\n"
            "The current directory is: {}.\n".format(os.getcwd()))
        self._update_config_file(
            file=self.configs_filepath,
            variable='CREDENTIALS_FILEPATH',
            replace_text=credentials_filepath)
        self.credentials_filepath = credentials_filepath

    def _update_config_file(self, file, variable, replace_text):
        import re
        f = open(file, "r")
        contents = f.read()
        new_contents = re.sub(
            r"{}\s*\:\s*.*\n".format(variable),
            "{}: '{}'\n".format(variable, replace_text),
            contents)
        f = open(file, "w")
        f.write(new_contents)
        f.close()

    def _get_db_bigquery_credentials(self):
        """Authenticate Google BigQuery API (i.e. google.cloud.bigquery).
        Returns the BigQuery client, which is added to bqClient.
        """
        from google.oauth2 import service_account
        self.database_credentials = \
            service_account.Credentials.from_service_account_file(
                self.credentials_filepath)
        return self.database_credentials