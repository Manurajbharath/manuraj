import datetime
import logging
import os
import sys
import uuid


import pandas as pd
from apiclient.http import MediaFileUpload
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.discovery import build
from libfb.py import db_locator
from libfb.py.db_locator import LocatorException
from libfb.py.mail import send_internal_email
from MySQLdb import cursors
from MySQLdb._exceptions import ProgrammingError


source_folder_id = ""
processed_passed_folder_id = ""
processed_failed_folder_id = ""
logger_folder_id = ""
data_folder_id = ""
SCOPES = ["https://www.googleapis.com/auth/drive"]
json_path = "your json path"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
f = logging.Formatter(
    "%(levelname)s-%(asctime)s-%(filename)s-%(funcName)s-%(lineno)s-%(threadName)s-%(message)s"
)
log_file_dir = os.environ.get("estimate_budget_upload_log")
log_file = log_file_dir + "estimatebudget.log"


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


def get_files_from_drive(creds, folder_id, mime_type="text/plain"):
    try:
        logger.info("Entering into the method")
        drive_service = build("drive", "v3", credentials=creds)
        results = (
            drive_service.files()
            .list(
                q="'" + folder_id + "' in parents and  mimeType='" + mime_type + "'",
                fields=" files(id, name, parents)",
            )
            .execute()
        )
        items = results.get("files", [])
        logger.info("Exiting from the method")
        return items
    except Exception as e:
        logger.exception(e)


def read_fileslist(creds):
    try:
        logger.info("Getting all files from given folder in GDrive")
        mime_type = "application/vnd.google-apps.spreadsheet"
        items = get_files_from_drive(creds, source_folder_id, mime_type)
        logger.debug("Number of files in the given folder are {0}".format(len(items)))
        if len(items) != 0:
            sheet_service = build("sheets", "v4", credentials=creds)
        for item in items:
            read_sheet(sheet_service, item.get("id"), item.get("name"))
        logger.info("Exiting from the method")
    except Exception as e:
        logger.exception("some error {0}".format(e))


def read_sheet(service, file_id, file_name):
    try:
        logger.info("Reading data from {0}".format(file_name))
        output = data_folder_id + file_name + "~" + str(uuid.uuid1()) + ".csv"
        RANGE_FOR_SHEET = file_name + "!A1:ZZ95248"
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(
                spreadsheetId=file_id,
                range=RANGE_FOR_SHEET,
            )
            .execute()
        )
        df = pd.DataFrame(result.get("values", []))
        df_replace = df.replace([None], [""])
        values = df_replace.values.tolist()
        logger.info("Read file {0} successfully".format(file_name))
    except Exception as e:
        logger.exception(f"Error reading spreadsheet{0}".format(e))

    if os.path.exists(output):
        logger.info("{0} File already exists".format(file_name))
        os.remove(output)
        logger.info("{0} file is removed".format(file_name))

    with open(output, "a+") as f:
        f.writelines("%s\n" % ",".join(i) for i in values)


def read_xdb(tier_name, table_name):
    xdb_data = None
    conn = None
    cur = None
    try:
        logger.info("Entering into method")
        locator = db_locator.Locator(tier_name, role="scriptrw")
        locator.do_not_send_autocommit_query()
        conn = locator.create_connection(cursorclass=cursors.DictCursor)
        cur = conn.cursor()
        logger.debug("Reading xdb given table:{0} ".format(table_name))
        ### files  from data  folder
        files_in_data_folder = []
        for filename in os.listdir(data_folder_id):
            files_in_data_folder.append("'" + filename + "'")
        if len(files_in_data_folder) > 0:
            data_files = ",".join(map(str, files_in_data_folder))
            statement = (
                "SELECT * FROM  "
                + table_name
                + " WHERE FILE_NAME IN ( "
                + data_files
                + " )"
            )
            cur.execute(statement)
            xdb_data = cur.fetchall()
            logger.debug("Rows read from table are {0}".format(xdb_data))
            logger.debug("Succesfully read from table {0}".format(table_name))
    except ProgrammingError as e:
        logger.debug("Table does not exist")
        logger.exception(e)
    except LocatorException as e:
        logger.debug("Schema/Tier/Shard does not exist")
        logger.exception(e)
    except Exception as e:
        logger.exception(e)
    finally:
        if conn and cur is not None:
            conn.commit()
            cur.close()
    logger.info("Exiting from method")
    return xdb_data


def move_files(creds, server_instance):  # 1
    try:
        logger.info("Moving files from source folder to processed folder")
        drive_service = build("drive", "v3", credentials=creds)
        mime_type = "application/vnd.google-apps.spreadsheet"
        items = get_files_from_drive(creds, source_folder_id, mime_type)
        files = read_xdb("xdb.gdcc_data", "estimate_budget_upload_audit_test")
        if files != None:
            files_list = list(files)
        if items is None or len(items) == 0 or files is None or len(files) == 0:
            logger.info(
                "There are no files in the source folder to move/There are  no files in data folder"
            )
        for item in items:
            status = ""
            for record in files_list:
                if (
                    item.get("name").strip()
                    == record["file_name"].split("~")[0].strip()
                ):
                    status = record["status"]
                    break
            previous_parents = ",".join(item.get("parents"))
            if status == "Success":
                file = (
                    drive_service.files()
                    .update(
                        fileId=item.get("id"),
                        addParents=processed_passed_folder_id,
                        removeParents=previous_parents,
                        fields="id, parents",
                    )
                    .execute()
                )
                logger.debug(
                    "{0} is moved succesfully to Passed folder {1}".format(
                        item.get("name"), processed_passed_folder_id
                    )
                )
                sendEmail(status, item.get("name"), server_instance)  # 2
            elif status == "Failed":
                file = (
                    drive_service.files()
                    .update(
                        fileId=item.get("id"),
                        addParents=processed_failed_folder_id,
                        removeParents=previous_parents,
                        fields="id, parents",
                    )
                    .execute()
                )
                logger.debug(
                    "{0} is moved succesfully to Failed folder {1}".format(
                        item.get("name"), processed_failed_folder_id
                    )
                )
                sendEmail(status, item.get("name"), server_instance)  # 3
        ## Delete files from data folder
        del_files_from_folder(data_folder_id)
        logger.info("Files in data folder are removed successfully")
    except Exception as e:
        logger.exception(e)


def del_files_from_folder(folder_path):
    try:
        for filename in os.listdir(folder_path):
            f = os.path.join(folder_path, filename)
            os.remove(f)
    except Exception as e:
        logger.exception(e)


def load_log(creds):
    try:
        logger.info("Loading log file....")
        drive_service = discovery.build("drive", "v3", credentials=creds)
        ct = datetime.datetime.now()
        ## read files from upload file directory
        for filename in os.listdir(log_file_dir):
            file_metadata = {
                "name": f"{filename}_{ct}_logfile",
                "parents": [logger_folder_id],
            }
            media = MediaFileUpload(
                os.path.join(log_file_dir, filename), mimetype="text/plain"
            )
            file = (
                drive_service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            logger.info("Log file is created with id {0}".format(file["id"]))
            ## delete files  from upload directory
            del_files_from_folder(log_file_dir)
        logger.info("Files in log folder are removed successfully")
    except Exception as e:
        logger.exception(e)


def sendEmail(status, file_name, server_instance):
    try:
        driveURL = "https://drive.google.com/drive/u/0/folders/" + getDriveURl(
            server_instance, status
        )

        #   to_emails = {"yogavm@fb.com", "rajmanu@fb.com", "animoen@fb.com", "sselvaraj@fb.com",
        # "josepulveda@fb.com", "winniehung@fb.com"}
        content = (
            """<html>
                        <head></head>
                        <body><h1 style='background-color: #33C0FF;font-size:24px;'>Estimate Budget Upload Status</h1>
                        Hello, <br> """
            + "Here is the recent Estimate Budget Upload Status "
            ""
            + """ <br>
                        <p><a href= """
            + driveURL
            + """ >
                        """
            + file_name
            + """</a>
                        :  """
            + status
            + """</p><br> <br>
                    <span> If you have any further questions please contact yogavm@fb.com </span><br><br>
                        <span>
                        Thanks,</span><br><br><span>GDCC Unifier Support Team</span>
                    </body>
                    </html>
                    """
        )

        to_emails = {}
        send_internal_email(
            sender="",
            to=to_emails,
            bcc="",
            subject="Status report - Estimate Budget Upload just testing",
            body=content,
            is_html=True,
        )
        logger.info("Mail has been sent successfully")
    except Exception as e:
        logger.exception(e)


def getDriveURl(instance, status):
    if instance.upper() == "TEST" and status.upper() == "SUCCESS":
        return os.environ.get("estimate_budget_test_pass")
    elif instance.upper() == "TEST" and status.upper() == "FAILED":
        return os.environ.get("estimate_budget_test_fail")
    elif instance.upper() == "PROD" and status.upper() == "SUCCESS":
        return os.environ.get("estimate_budget_prod_pass")
    elif instance.upper() == "PROD" and status.upper() == "FAILED":
        return os.environ.get("estimate_budget_prod_fail")
    return (
        ""
    )


if __name__ == "__main__":

    if len(sys.argv) != 3:
        logger.error(
            "Please provide correct environment and input appropriate command (Read/Move/Loadlog)"
        )
        exit(1)
    creds = get_creds()
    if sys.argv[1].upper() == "TEST":
        source_folder_id = os.environ.get("estimate_budget_test_source")
        processed_passed_folder_id = os.environ.get("estimate_budget_test_pass")
        processed_failed_folder_id = os.environ.get("estimate_budget_test_fail")
        logger_folder_id = os.environ.get("estimate_budget_test_log")
    elif sys.argv[1].upper() == "PROD":
        source_folder_id = os.environ.get("estimate_budget_prod_source")
        processed_passed_folder_id = os.environ.get("estimate_budget_prod_pass")
        processed_failed_folder_id = os.environ.get("estimate_budget_prod_fail")
        logger_folder_id = os.environ.get("estimate_budget_prod_log")
    else:
        logger.error("Please provide correct environment")
        exit(1)

    if sys.argv[2].upper() == "READ":
        read_fileslist(creds)
    elif sys.argv[2].upper() == "MOVE":
        move_files(creds, sys.argv[1])  # 4
    elif sys.argv[2].upper() == "LOADLOG":
        load_log(creds)
    else:
        logger.error("Please input appropriate command (Read/Move/Loadlog)")
        exit(0)
