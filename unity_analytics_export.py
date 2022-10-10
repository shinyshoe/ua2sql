import datetime
import gzip
import io
import json
import os
import shutil
import sys
import time

import requests
from requests.auth import HTTPBasicAuth

if len(sys.argv) < 4:
    print('please provide path to configuration file and start-end dates. see README.md for specs.')
    exit(1)

CONFIG = {}

try:
    with open(sys.argv[1]) as f:
        CONFIG = json.load(f)
except:
    print('failed to read or parse config file: ' + sys.argv[1])
    exit(1)

if not CONFIG['collection_path'] or \
    not CONFIG['unity_project_id'] or \
    not CONFIG['unity_export_api_key']:
    print("missing parameter in config.json. see docs.")
    exit(1)

START_DATE = sys.argv[2] 
END_DATE = sys.argv[3]

# figure out home directory if necessary
CONFIG['collection_path'] = os.path.expanduser(CONFIG['collection_path'])

# start a request for Unity analytics data and return the ID for the job
def request_raw_analytics_dump(unity_project_id, unity_api_key, start_date, end_date, dump_format, data_set,
                               continue_from=None):
    uri = 'https://analytics.cloud.unity3d.com/api/v2/projects/' + unity_project_id + '/rawdataexports'

    postBodyJson = {'endDate': end_date, 'format': dump_format, 'dataset': data_set}

    if continue_from is not None:
        postBodyJson['continueFrom'] = continue_from
    else:
        postBodyJson['startDate'] = start_date

    headers = {'content-type': 'application/json'}
    r = requests.post(uri, json.dumps(postBodyJson), auth=HTTPBasicAuth(unity_project_id, unity_api_key),
                      headers=headers)

    if r.status_code == 200:
        return r.json()['id']

    return None


# checks whether or not a Unity dump job is done or not
def is_raw_analytics_dump_ready(unity_project_id, unity_api_key, job_id):
    uri = 'https://analytics.cloud.unity3d.com/api/v2/projects/' + unity_project_id + '/rawdataexports/' + job_id
    r = requests.get(uri, auth=HTTPBasicAuth(unity_project_id, unity_api_key))

    if r.status_code == 200:
        return r.json()['status'] == 'completed'

    return False


# extracts and un-compresses all result files in a job
def save_raw_analytics_dump(unity_project_id, unity_api_key, job_id, destination_directory):
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)

    uri = 'https://analytics.cloud.unity3d.com/api/v2/projects/' + unity_project_id + '/rawdataexports/' + job_id
    r = requests.get(uri, auth=HTTPBasicAuth(unity_project_id, unity_api_key))

    if r.status_code != 200:
        print('unable to retrieve result due to HTTP error: ' + r.status_code)
        print('URI: ' + uri)
        return

    responseJson = r.json()

    if responseJson['status'] != 'completed':
        print('job status not completed... can\'t dump results yet')
        return

    if 'fileList' not in responseJson['result']:
        print('no files for job: ' + job_id)
        return

    for fileToDownload in responseJson['result']['fileList']:
        fileUri = fileToDownload['url']
        fileName = os.path.splitext(fileToDownload['name'])[0]  # file name w/o extension

        fileRequest = requests.get(fileUri)

        if fileRequest.status_code == 200:
            compressed_file = io.BytesIO(fileRequest.content)
            decompressed_file = gzip.GzipFile(fileobj=compressed_file)

            with open(os.path.join(destination_directory, fileName), 'w+b') as outFile:
                outFile.write(decompressed_file.read())


# removes all files in a directory. does not recurse.
def remove_files_in_directory(path):
    for filename in os.listdir(path):
        full_path = os.path.join(path, filename)
        if os.path.isfile(full_path):
            os.remove(full_path)


# copies raw dump results to a backup folder named after the job type and today's date
def backup_job_results(job_type, local_dump_directory, remote_dump_directory_root):
    src_files = os.listdir(local_dump_directory)

    # don't bother making a backup folder if there are no files for this type
    if len(src_files) == 0:
        return

    # make folders if they aren't there yet
    destination_path = os.path.join(remote_dump_directory_root, job_type, str(datetime.date.today()))
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    for file_name in src_files:
        full_file_name = os.path.join(local_dump_directory, file_name)
        shutil.copy2(full_file_name, os.path.join(destination_path, file_name))

    print('backed up raw dump to path: ' + destination_path)


# ties it all together - downloads a dump, backs it up, and inserts it into the database
def process_raw_dump(job_type, local_dump_directory, start_date, end_date):
    print('collector: starting collection for job: ' + job_type)

    today = datetime.date.today()
    jobId = request_raw_analytics_dump(CONFIG['unity_project_id'],
                                       CONFIG['unity_export_api_key'],
                                       start_date, end_date,
                                       'json', job_type)

    print('started jobId: ' + jobId)

    while not is_raw_analytics_dump_ready(CONFIG['unity_project_id'],
                                          CONFIG['unity_export_api_key'], jobId):
        time.sleep(5)

    # make folders if they aren't there yet
    destination_path = os.path.join(local_dump_directory, start_date + "_" + end_date)
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    save_raw_analytics_dump(CONFIG['unity_project_id'], CONFIG['unity_export_api_key'], jobId,
                            destination_path)
    print('done! all results for job ' + job_type + ' saved to: ' + destination_path)

print('')
print(str(datetime.datetime.now()))
print(f"""*** STARTING COLLECTION {START_DATE} - {END_DATE} ***""")

process_raw_dump('custom', CONFIG['collection_path'], START_DATE, END_DATE)

print('*** COMPLETE ***')
