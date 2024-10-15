#!/usr/bin/env python
import os
import datetime
import asyncio
from dotenv import load_dotenv
from pprint import pprint
from google.cloud import storage_transfer
from google.cloud import storage_transfer_v1
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

load_dotenv() # Load environment variables from .env file

# Reused variables should go here
project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
current_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")

# [ START export_sql_dump ]
def export_sql_dump():
  credentials = GoogleCredentials.get_application_default()

  service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)

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

  print("Exporting SQL dump, from instance: ", instance_name)
  pprint(response)
# [ END export_sql_dump ]

# [ START transfer_to_archive_storage ]
async def transfer_to_archive_storage():
  
  export_sql_dump()

  client = storage_transfer.StorageTransferServiceClient()
  asyncClient = storage_transfer.StorageTransferServiceAsyncClient()

  dest_bucket_name = os.getenv('DEST_BUCKET_NAME')
  transfer_job_name = os.getenv('TRANSFER_JOB_NAME')

  # Check if the transfer job already exists
  filter_string = f'{{"projectId":"{project_id}"}}'

  list_transfer_jobs_request = storage_transfer.ListTransferJobsRequest(
          filter=filter_string,
  )
  existing_jobs = client.list_transfer_jobs(request=list_transfer_jobs_request)

  if existing_jobs:
    print("Existing transfer job found, starting transfer job")
  else:
    print("No existing transfer job found, creating new transfer job")
    create_transfer_job_request = storage_transfer.CreateTransferJobRequest( 
      {
        "transfer_job": {
          "name": f"transferJobs/{transfer_job_name}",
          "project_id": f"{project_id}",
          "status": storage_transfer.TransferJob.Status.ENABLED,
          "transfer_spec": {
            "gcs_data_source": {
              "bucket_name": f"{bucket_name}",
            },
            "gcs_data_sink": {
              "bucket_name": f"{dest_bucket_name}",
            },
          },
        }
      }
    )

    create_response = await asyncClient.create_transfer_job(request=create_transfer_job_request)
    print(f"Created transfer job: {create_response.name}")

  # Start the transfer job
  run_transfer_job_request = storage_transfer_v1.RunTransferJobRequest(
    {
      "job_name": f"transferJobs/{transfer_job_name}",
      "project_id": f"{project_id}",
    }
  )

  operation = await asyncClient.run_transfer_job(request=run_transfer_job_request)
  
  print("Transfer job started...")
  # Handle the response
  response = await operation.result()
  print("Transfer job complete.", response)

# [ END transfer_to_archive_storage ]
asyncio.run(transfer_to_archive_storage())