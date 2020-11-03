from google.cloud import storage
from google.cloud import bigquery
import pickle
import os
import pandas as pd
import pandas_gbq



class CloudService(object):
    def __init__(self, project=None):
        super(CloudService, self).__init__()
        self.project = project
        if project is not None:
            self.client = bigquery.Client(project=project)
            self.storage_client = storage.Client(project=project)
        else:
            self.client = bigquery.Client()
            self.storage_client = storage.Client()

    def read_gbq(self, query_string):
        df = self.client.query(query_string).result().to_dataframe(progress_bar_type='tqdm')
        return df
    def write_gbq(self, data, project_id, table_id):
        pandas_gbq.to_gbq(data, destination_table = table_id, project_id = project_id, if_exists = 'append')
    def read_write_gbq(self, query_string, project_id, table_id):
        data = self.read_gbq(query_string)
        self.write_gbq(data = data, project_id= project_id, table_id= table_id)

    def create_table(self, query_string, project, dataset, table):
        query_string = f"CREATE OR REPLACE TABLE `{project}.{dataset}.{table}` AS\n" + query_string
        print('query_string ', query_string)
        self.client.query(query_string).result()
        print(f'Done Create OR REPLACE `{project}.{dataset}.{table}`')
        return True

    def download_frombgtogcs(self, project, dataset_id, table_id, bucket_name, source_blob_name):
        for blob_name in self.list_blobs(bucket_name=bucket_name, prefix=source_blob_name):
            if '.' in blob_name:
                self.delete_blob(bucket_name=bucket_name, blob_name=blob_name)
        destination_uri = "gs://{}/{}/data-*.csv".format(bucket_name, source_blob_name)
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)
        extract_job = self.client.extract_table(
            table_ref,
            destination_uri,
            location="US",
        )  # API request
        extract_job.result()
        print("Exported {}:{}.{} to {}".format(self.project, dataset_id, table_id, destination_uri))
        return True

    def list_blobs(self, bucket_name, prefix=None):
        blobs = self.storage_client.list_blobs(bucket_name, prefix=prefix)
        results = []
        for blob in blobs:
            results.append(blob.name)
        return results

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # check file exist
        check = False
        blob_lst = self.storage_client.list_blobs(bucket_name, prefix=source_blob_name)
        for blob in blob_lst:
            if source_blob_name == blob.name:
                check = True

        if check == True:
            blob.download_to_filename(destination_file_name)
            print("Blob {} downloaded to {}".format(source_blob_name, destination_file_name))
        else:
            print('Blob {} not downloaded to {}'.format(source_blob_name, destination_file_name))

    def delete_blob(self, bucket_name, blob_name):
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        print('Blob {} deleted.'.format(blob_name))

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print('File {} uploaded to {}'.format(source_file_name, destination_blob_name))

    def download_object(self, bucket_name, gcs_file_name, local_file_name):
        self.download_blob(bucket_name, gcs_file_name, local_file_name)
        with open(local_file_name, 'rb') as fp:
            obj = pickle.load(fp)
        return obj

    def download_multipleobject(self, bucket_name, gcs_folder_name, local_folder_name):

        for blob_name in self.list_blobs(bucket_name=bucket_name, prefix=gcs_folder_name):
            if 'csv' in blob_name:
                self.download_blob(bucket_name, blob_name, os.path.join(local_folder_name, blob_name.split('/')[-1]))
        dfs = []
        for blob_name in os.listdir(local_folder_name):
            obj = pd.read_csv(os.path.join(local_folder_name, blob_name))
            dfs.append(obj)
        return pd.concat(dfs)

    def upload_object(self, object_name, bucket_name, local_file_name, gcs_file_name, format_file_name=None):
        try:
            if format_file_name == 'df':
                object_name.to_pickle(local_file_name)
            else:
                with open(local_file_name, 'wb') as fp:
                    pickle.dump(object_name, fp, protocol=pickle.HIGHEST_PROTOCOL)

            self.upload_blob(bucket_name, local_file_name, gcs_file_name)
        except:
            print('Fail upload object')
            return False
        return True
    def dump_object(self, object_name, local_file_name, format = None):
        try:
            if format == 'df':
                object_name.to_pickle(local_file_name)
            else:
                with open(local_file_name, 'wb') as fp:
                    pickle.dump(object_name, fp, protocol=pickle.HIGHEST_PROTOCOL)

        except:
            print('Fail dump object {}'.format(local_file_name))
            return False
        return True
