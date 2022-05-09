import datetime
import io
import logging
import os
import shutil
import sys

# import requests
import pandas as pd
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.discovery import build
from libfb.py import db_locator


source_folder_id = ""
SCOPES = [
    "https://www.googleapis.com/auth/drive",
]
json_path = (
    ""
)
upload_file = "g"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
f = logging.Formatter(
    "%(levelname)s-%(asctime)s-%(filename)s-%(funcName)s-%(lineno)s-%(threadName)s-%(message)s"
)

log_file_dir = (
    ""
)
log_file = log_file_dir + "gdrive_unifier.log"
fh = logging.FileHandler(log_file)
fh.setFormatter(f)
logger.addHandler(fh)


def get_creds():
    logger.info("Getting credentials")
    # JSON key file
    try:
        SERVICE_ACCOUNT_FILE = json_path
        if os.path.exists(json_path):
            logger.info("Token file exists")
    except Exception as e:
        logger.exception(f"Token file does not exists {0}".format(e))

    credentials = None
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    logger.info("Pulled credentials from token file")
    return credentials


def get_files_from_drive(creds, folder_id=source_folder_id):
    try:
        logger.info("Entering into the method")
        drive_service = build("drive", "v3", credentials=creds)
        results = (
            drive_service.files()
            .list(
                q="'" + folder_id + "' in parents ",
                fields=" files(id, name, parents, mimeType)",
            )
            .execute()
        )
        items = results.get("files", [])
        logger.info(len(items))
        for item in items:
            if item.get("mimeType") != "application/vnd.google-apps.folder":
                download_files(
                    drive_service,
                    item.get("id"),
                    item.get("name"),
                    item.get("mimeType"),
                )
            else:
                get_files_from_drive(creds, item.get("id"))
        logger.info("Exiting from the method")
    except Exception as e:
        logger.exception(e)


def download_files(drive_service, file_id, file_name, mime_type):
    try:
        logger.info("Entering into the method")
        path = (
            ""
        )
        if mime_type == "application/vnd.google-apps.spreadsheet":
            download_gsheet(file_id, file_name, path)
        else:
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fd=fh, request=request)
            done = False
            while done is False:
                status, done = downloader.next_chunk(num_retries=1000)
            fh.seek(0)
            file_name = os.path.join(path, file_name)
            f = open(file_name, "wb")
            f.write(fh.read())
            f.close()
        return file_name
        logger.info("Exiting from the method")
    except Exception as e:
        logger.exception(e)


def download_gsheet(file_id, file_name, path):
    try:
        logger.info("Reading data from {0}".format(file_name))
        sheet_service = build("sheets", "v4", credentials=creds)
        RANGE_FOR_SHEET = file_name + "!A1:ZZ95248"
        result = (
            sheet_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=file_id,
                range=RANGE_FOR_SHEET,
            )
            .execute()
        )
        df = pd.DataFrame(result.get("values", []))
        df_replace = df.replace([None], [""])
        output = path + file_name + ".xlsx"
        writer = pd.ExcelWriter(output, engine="xlsxwriter")
        df_replace.to_excel(writer, sheet_name=file_name, index=False, header=False)
        writer.save()
        logger.info("Read file {0} successfully".format(file_name))
    except Exception as e:
        logger.exception(f"Error reading spreadsheet{0}".format(e))


if __name__ == "__main__":

    if len(sys.argv) != 2:
        logger.info("Please input appropriate command (Read/Move/Loadlog)")
        exit(1)
    creds = get_creds()
    if sys.argv[1].upper() == "READ":
        get_files_from_drive(creds)
    else:
        logger.info("Please input appropriate command (Read/Move/Loadlog)")
        exit(0)
