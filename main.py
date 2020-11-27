from cloudservice import CloudService

cloud = CloudService(project = 'vinid-data-science-prod')

query_string = """
select SHA1(user_id) as user_id, * except(user_id)
from `vinid-data-science-prod.P13N_CAMPAIGN.ALL_NR_ORDERS`
inner join `vinid-data-science-prod.P13N_CAMPAIGN_TEMP.LESS_ACTIVE_SEGMENTS_2810`
using (user_id)
where calendar_dim_id between date_sub('2020-10-27', interval 90 day) and '2020-10-27'
and main_domain = 'retails'
"""
project_id = 'vinid-playground'
table_id = 'thiepnv.LESS_ACTIVE_NR_ORDERS'
cloud.read_write_gbq(query_string,
                     project_id=project_id,
                     table_id = table_id)
