from cloudservice.cloud import CloudService

cloud = CloudService(project = 'vinid-data-science-prod')


query_string = """
select *
from `vinid-data-science-prod.P13N_CAMPAIGN.P13N_MODEL_OUTPUT` LIMIT 1000
"""
cloud.read_write_gbq(query_string,
                     project_id='vinid-data-science-prod',
                     table_id = 'P13N_CAMPAIGN_TEMP.LOG_TEST_CLOUD_READ_WRITE_GBQ')
