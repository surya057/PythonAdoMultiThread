import os
import uuid
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import json
import adlfs

def get_headers_api():
    access_token = ""
    headers_api = {
        'accept': 'application/json',
        'Authorization': 'Basic ' + access_token,
    }
    return headers_api


def download_build_to_blob(project_id, min_time, max_time, org, storage_account_url, container_name, sas_token):
    formatted_date = min_time.strftime("%Y-%m-%d")
    build_url = f'https://dev.azure.com/{org}/{project_id}/_apis/build/builds?api-version=7.0&$top=2000&statusFilter=completed&minTime={min_time}&maxTime={max_time}&queryOrder=finishTimeDescending'
    build_response = requests.get(build_url, headers=get_headers_api())
    build_json_response = build_response.json()
    
    if build_json_response:
        for build in build_json_response['value']:
            build_id = build['id']
            url_build_timeline = f'https://dev.azure.com/{org}/{project_id}/_apis/build/builds/{build_id}/Timeline'
            build_timeline_response = requests.get(url_build_timeline, headers=get_headers_api())
            build_timeline_json_response = build_timeline_response.json()
            
            json_string = json.dumps(build_timeline_json_response)
            file_path = f"controls-build-timelines/{formatted_date}/{org}/{project_id}_{build_id}.json"
            fs = adlfs.AzureBlobFileSystem(
                account_name="",
                account_url=storage_account_url,
                sas_token=sas_token
            )

            with fs.open(f"{container_name}/{file_path}", "w") as f:
                f.write(json_string)
            print(f"Uploading for --> {formatted_date}")


def download_file():
    try:
        org = ""
        endpoint = f'https://dev.azure.com/{org}/_apis/projects?$top=7500&api-version=7.0&?stateFilter=all'
        response = requests.get(endpoint, headers=get_headers_api())
        
        if response.status_code != 200:
            raise Exception(f'The API call to endpoint {endpoint} has failed: {response.reason}')
        
        json_response = response.json()
        print(json_response['value'][0]['id'])
        start_date = '2023-01-01'
        end_date = '2023-07-11'
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        number_of_days = (end - start).days
        
        threads = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            for i in range(number_of_days):
                for project in json_response['value']:
                    project_id = project['id']
                    min_time = start + timedelta(days=i)
                    max_time = start + timedelta(days=i+1)
                    threads.append(executor.submit(download_build_to_blob, project_id, min_time, max_time, org, storage_account_url, container_name, sas_token))
        
        return "success"
    except requests.exceptions.RequestException as e:
        print(e)
        return e

if __name__ == "__main__":
    storage_account_url = ""
    container_name = "landing"
    sas_token = ""
    download_file()
