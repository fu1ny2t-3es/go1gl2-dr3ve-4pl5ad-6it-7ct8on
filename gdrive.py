import os
import json
import sys
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from tqdm import tqdm
from datetime import datetime


scope = "https://www.googleapis.com/auth/drive"
credentials_input = "credentials"


def _substr(str, idx0, pat1, pat2):
    idx1 = str.find(pat1, idx0)
    if pat1 == '':
        idx1 = idx0
    elif idx1 == -1:
        return None, -1

    idx1 += len(pat1)
    idx2 = str.find(pat2, idx1)

    if idx2 == -1:
        return None, -1

    return str[idx1:idx2], idx1



def gdrive(drive_json, drive_folder):
    with open(drive_json + '.json') as f:
        creds = json.load(f)


    # fetching a JWT config with credentials and the right scope
    try:
        credentials = service_account.Credentials.from_service_account_info(
            creds, scopes=[scope]
        )
    except Exception as err:
        logging.error(f"fetching JWT credentials failed with error: {err}")
        sys.exit(1)

    # instantiating a new drive service
    service = build('drive', 'v3', credentials=credentials)


	#########################################################


    try:
        r = service.files().list(
            q="'me' in owners",
            fields="files(id,name,size),nextPageToken",
            orderBy="name",
            pageSize=1000,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()

    except err:
        print("Unable to retrieve files: %s", err)
        sys.exit(1)


    # print('Files:')

    if len(r.get('files', [])) != 0:
        for i in r['files']:
            if i['name'].startswith("#@__"):
                print(f"Erasing ###  {i['name']}   {i['id']}")

                err = service.files().delete(fileId=i['id']).execute()
                if err:
                    print("deleting file failed with error: " + err)
            else:
                print(f"{i['name']}      {i['id']}")


    about = service.about().get(fields="storageQuota").execute()
    quota = about['storageQuota']


	#########################################################


    src_id = ''  # copy folder
    dst_id = drive_folder


    try:
        r = service.files().list(
            q=f"'{src_id}' in parents",
            fields="files(id,name,size),nextPageToken",
            orderBy="name",
            pageSize=1000,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()

    except Exception as err:
        print('Unable to retrieve files: ' + err)
        sys.exit(1)


    if 'files' in r and len(r['files']) > 0:
        # print('Copying:')

        for i in r['files']:
            about = service.about().get(fields="storageQuota").execute()
            quota = about['storageQuota']

            drive_size = int(quota['limit']) - int(quota['usage'])

            if (int(i['size']) < drive_size) and (int(i['size']) > 0) and not i['name'].startswith("@__") and not i['name'].startswith("#@__"):
                copy_file = service.files().get(fileId=i['id']).execute()

                if copy_file:
                    copy_file = {'parents': [dst_id]}

                    try:
                        service.files().copy(fileId=i['id'], body=copy_file).execute()
                        print('Copied  ' + i['name'] + '     ' + i['id'])

                        moved_file = {'name': '@__' + i['name']}
                        service.files().update(fileId=i['id'], body=moved_file).execute()

                    except Exception as err:
                        print(err)
                        print(f"File error {i['name']}  [{i['size']} / {drive_size}]")

                else:
                    print(f"File error {i['name']}  [{i['size']} / {drive_size}]")


	#########################################################


    about = service.about().get(fields="storageQuota").execute()
    quota = about['storageQuota']

    if (int(quota['usage']) >= int(quota['limit'])):
        print(quota['usage'] + '  ' + quota['limit'] + '  ###  ' + 'Storage exceeded:  ' + drive_folder)
        sys.exit(1)



def missing_input(input_name):
    logging.error(f"missing input '{input_name}'")
    sys.exit(1)



def main():
    try:
        with open(os.path.join('keys', 'keys.txt'), 'r', encoding='utf-8') as f:
            for line in f.read().splitlines():
                drive_json = _substr(line, 0, '', '.json')[0]
                drive_folder = _substr(line, 0, 'https://drive.google.com/drive/folders/', '?usp=sharing')[0]

                print(drive_json + '     ' + drive_folder)
                gdrive(os.path.join('keys', drive_json), drive_folder)

                print('')
                print('')
                print('')
                print('')

    except Exception as err:
        print(err)



if __name__ == "__main__":
    old = datetime.now()

    main()

    print(old)
    print(datetime.now())
