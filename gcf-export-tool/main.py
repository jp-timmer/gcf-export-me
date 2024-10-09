#!/usr/bin/env python
import os
import datetime
from dotenv import load_dotenv
from pprint import pprint
from google.cloud import storage_transfer
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

load_dotenv()

# [ START export_sql_dump ]
def export_sql_dump():
  credentials = GoogleCredentials.get_application_default()

  service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

  project_id = os.getenv('PROJECT_ID')
  bucket_name = os.getenv('BUCKET_NAME')
  current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
  instance_name = os.getenv('INSTANCE_NAME')
  database_name = os.getenv('DATABASE_NAME')

  instances_export_request_body = {
    "exportContext": {
      "kind": "sql#exportContext",
      "fileType": "SQL",
      "uri": f"gs://{bucket_name}/{current_date}-export.sql",
      "databases": [f"{database_name}"],
    }
  }

  request = service.instances().export(project=project_id, instance=instance_name, body=instances_export_request_body)
  response = request.execute()

  pprint(response)
# [ END export_sql_dump ]

def transfer_to_archive_storage():

  client = storage_transfer.StorageTransferServiceClient()

  transfer_job_request = storage_transfer.CreateTransferJobRequest(
    {
      # add transfer job
    }
  )

  result = client.create_transfer_job(transfer_job_request)
  print(f"Created transfer job: {result.name}")

export_sql_dump()