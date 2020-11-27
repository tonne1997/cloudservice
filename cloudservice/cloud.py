from google.cloud import storage
from google.cloud import bigquery
import pickle
import os
from pathlib import Path
from tqdm.auto import tqdm
import pandas as pd
import pickle
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

    def read_gbq(self, query):
        df = self.client.query(query).result().to_dataframe(progress_bar_type='tqdm')
        return df
    def write_gbq(self, data, project_id, destination_table, **kwargs):
        pandas_gbq.to_gbq(data, destination_table = table_id, project_id = project_id, **kwargs)

    def create_table(self, query_string, project, dataset, table):
        query_string = f"CREATE OR REPLACE TABLE `{project}.{dataset}.{table}` AS\n" + query_string
        print('query_string ', query_string)
        self.client.query(query_string).result()
        print(f'Done Create OR REPLACE `{project}.{dataset}.{table}`')
        return True

    def list_blobs(self, bucket_name, prefix = None, delimiter = None):
        result = []
        blobs = self.storage_client.list_blobs(bucket_name, prefix = prefix, delimiter = delimiter)
        for blob in blobs:
            result.append(blob.name)
        # if delimiter:
        #     for prefix in blobs.prefixes:
        #         print('prefix: ', prefix)
        return result

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        # check file exist
        blob_list = self.list_blobs(bucket_name, prefix=source_blob_name, delimiter = '/')
        check = sum([True if source_blob_name in file_name else False for file_name in blob_list ])
        if check > 0:
            blob.download_to_filename(destination_file_name)
            print("[gcs]: {} downloaded to [local]: {}".format('gs://' + str(Path(bucket_name, source_blob_name)), destination_file_name))
        else:
            print('[gcs]: {} not downloaded to [local]: {}'.format(source_blob_name, destination_file_name))

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

    def download_file(self, bucket_name, gcs_filepath, local_filepath):
        if gcs_filepath[-1] == '/' and local_filepath[-1] == '/':
            Path(local_filepath).mkdir(parents = True, exist_ok=True)
            list_file = self.list_blobs(bucket_name=bucket_name, prefix = gcs_filepath, delimiter='/')
            for file in list_file:
                file_name = file.split('/')[-1]
                source_blob_name = str(Path(gcs_filepath, file_name))
                destination_file_name = str(Path(local_filepath, file_name))
                self.download_blob(bucket_name = bucket_name, source_blob_name = source_blob_name, destination_file_name=destination_file_name)
        elif gcs_filepath[-1] != '/' and local_filepath[-1] != '/':
            Path('/'.join(local_filepath.split('/')[:-1])).mkdir(parents = True, exist_ok=True)
            self.download_blob(bucket_name, gcs_filepath, local_filepath)
        else:
            print('File not download')
    def read_filename(self, file_name, format = 'pickle'):
        if format == 'pickle':
            try:
                data = pd.read_pickle(file_name)
            except:
                data = pickle.load(open(file_name, 'rb'))
        elif format == 'csv':
            data = pd.read_csv(file_name)

        elif format == 'parquet':
            data = pd.read_parquet(file_name)
        elif format == 'json':
            data = pd.read_json(file_name, lines = True)
        return data

    def download_object(self, bucket_name, gcs_filepath, local_filepath):
        self.download_file(bucket_name = bucket_name, gcs_filepath = gcs_filepath, local_filepath = local_filepath)
        result = []
        for file_name in Path(local_filepath).glob('*.*'):
            file_name = str(file_name)
            print('file_name: ', file_name)
            auto_format = file_name.split('.')[-1]
            if auto_format == 'pkl':
                auto_format = 'pickle'
            print('format: ', auto_format)
            result.append(self.read_filename(file_name, format = auto_format))
        return pd.concat(result, axis = 0)

    def download_frombgtogcs(self, project, dataset_id, table_id, bucket_name, source_blob_name, localtion = 'US'):
        for blob_name in self.list_blobs(bucket_name=bucket_name, prefix=source_blob_name):
            if '.' in blob_name:
                self.delete_blob(bucket_name=bucket_name, blob_name=blob_name)
        destination_uri = "gs://" + str(Path(bucket_name, source_blob_name, 'data-*.json'))
        dataset_ref = bigquery.DatasetReference(project = project, dataset_id = dataset_id)
        table_ref = dataset_ref.table(table_id)
        configuration = bigquery.ExtractJobConfig()
        configuration.destination_format='NEWLINE_DELIMITED_JSON'
        extract_job = self.client.extract_table(
                                                table_ref,
                                                destination_uri,
                                                location = localtion,
                                                job_config = configuration
        )
        extract_job.result() # Waits for job to complete
        print("Exported {}:{}.{} to {}".format(self.project, dataset_id, table_id, destination_uri))
        return True

    def read_gbq2(self, query, project, dataset_id, table_id, bucket_name, gcs_filepath, local_filepath):
        # create table
        query = f"CREATE OR REPLACE TABLE `{project}.{dataset_id}.{table_id}` AS\n" + query
        self.client.query(query).result()
        self.download_frombgtogcs(project = project, dataset_id = dataset_id, table_id = table_id, bucket_name = bucket_name, source_blob_name =  gcs_filepath)
        if gcs_filepath[-1] != '/':
            gcs_filepath = gcs_filepath + '/'
        if local_filepath[-1] != '/':
            local_filepath = local_filepath + '/'
        df = self.download_object(bucket_name = bucket_name, gcs_filepath = gcs_filepath, local_filepath = local_filepath)
        return df


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
    def to_object(self, obj, local_filepath, format = 'pickle'):
        try:
            if format == 'pickle' or format == 'pkl':
                try:
                    obj.to_pickle(local_filepath)
                except:
                    with open(local_filepath, 'wb') as fp:
                        pickle.dump(obj, fp, protocol=pickle.HIGHEST_PROTOCOL)
            elif(format == 'csv'):
                obj.to_csv(local_filepath, index=False)
            elif(format == 'parquet'):
                obj.to_parquet(local_filepath, index=False)
            return True
        except:
            print('[ERROR] to object {}'.format(local_filepath))
            return False


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
if __name__=='__main__':
    cloud = CloudService(project='vinid-data-science-prod')
    query = """
select *
from `vinid-data-science-prod.P13N_CAMPAIGN.P13N_MODEL_OUTPUT`
where 1=1
and calendar_dim_id > '2020-11-24'
and promotion_detail_absolute_discount is not null
    """
    import time
    df = cloud.read_gbq2(query = query,
                         project = 'vinid-data-science-prod',
                         dataset_id = 'P13N_CAMPAIGN_TEMP',
                         table_id = 'TEST_CLOUDSERVICE',
                         bucket_name = 'data-p13n-campaign',
                         gcs_filepath='production/2020-11-27/testne',
                         local_filepath = './data/2020-11-27/testne')
    print('df: ', df)
    print('shape: ', df.shape)
    # print('Start list_blobs')
    # cloud.download_file('data-p13n-campaign', gcs_filepath = 'production/2020-11-27/data/action_recommendation_active.pkl', local_filepath = './data/action_recommendation_active.pkl')
