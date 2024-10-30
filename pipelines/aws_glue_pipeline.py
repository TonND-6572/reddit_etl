from etls.aws_glue_service import glue_crawler, connect_to_botos3

def upload_glue_pipeline(client:str, crawler_name:str):
    # run_id = glue_etl(job_name)
    # while not etl_done(job_name=job_name, run_id=run_id):
    client = connect_to_botos3(client)
    glue_crawler(client, crawler_name)