#!/usr/bin/env python3

import shutil
import sys
import os
import datetime
import io

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials


BACKUP_FOLDER_NAME = "solarpi_backups"


def main():
    try:
        db_path = os.environ["DB_PATH"]
        user_email = os.environ["USER_EMAIL"]
    except KeyError as err:
        print("Environment variable(s) not set!")
        print("Expected 'DB_PATH' and 'USER_EMAIL' to be set.")
        print("Exiting...")
        sys.exit(1)

    if "RESTORE_DB" in os.environ:
        restore_last(db_path)
    else:
        backup(db_path, user_email)


def backup(db_path, user_email):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "drive_credentials.json", scopes=["https://www.googleapis.com/auth/drive.file"])
    service = build('drive', 'v3', credentials=credentials)

    response = service.files().list(q="mimeType='application/vnd.google-apps.folder'",
                                    fields="files(id, name)").execute()
    folders = response.get("files", [])
    backup_folder = list(filter(lambda folder: folder['name'] == BACKUP_FOLDER_NAME, folders))
    if not backup_folder:
        response = service.files().create(
            body={"name": BACKUP_FOLDER_NAME, "mimeType": 'application/vnd.google-apps.folder'},
            fields="id").execute()
        folder_id = response.get("id")
        service.permissions().create(
            fileId=folder_id, body={"type": "user", "role": "writer",
                                    "emailAddress": user_email}).execute()
    else:
        folder_id = backup_folder[0]['id']

    backup_name = "solarpi_backup_{}".format(datetime.datetime.now(datetime.timezone.utc).date())

    print("Packing file...")
    shutil.make_archive(backup_name, "gztar", db_path)
    print("Uploading...")
    media = MediaFileUpload(backup_name + ".tar.gz", resumable=True)
    service.files().create(body={"name": backup_name + ".tar.gz", "parents": [folder_id]},
                           media_body=media, fields='').execute()
    print("Done")


def restore_last(db_path):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "drive_credentials.json", scopes=["https://www.googleapis.com/auth/drive.file"])
    service = build('drive', 'v3', credentials=credentials)

    response = service.files().list(q="mimeType='application/x-tar'", orderBy="createdTime desc",
                                    pageSize=10, fields="files(id, name)").execute()
    files = response.get("files", [])
    if not files:
        print("Could not find any backup file!")
        return
    file = files[0]
    print("Downloading file:", file['name'])
    request = service.files().get_media(fileId=file['id'])
    fh = io.FileIO(file['name'], mode='wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))

    print("Unpacking...")
    shutil.unpack_archive(file['name'], db_path)
    print("Done")


if __name__ == "__main__":
    main()
