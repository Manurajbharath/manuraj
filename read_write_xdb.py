import logging
import os


from libfb.py import db_locator
from libfb.py.db_locator import LocatorException
from MySQLdb import cursors
from MySQLdb._exceptions import ProgrammingError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
f = logging.Formatter(
    "%(levelname)s-%(asctime)s-%(filename)s-%(funcName)s-"
    "%(lineno)s-%(thread)s-%(threadName)s-%(message)s"
)


directory = os.getcwd()
cwd = ""
logfile = os.path.join(directory, cwd, "readwrite.log")
fh = logging.FileHandler(logfile)
fh.setFormatter(f)
logger.addHandler(fh)
# consoleHandler = logging.StreamHandler()
# consoleHandler.setFormatter(f)
# logger.addHandler(consoleHandler)


class Db_Operations:
    def __init__(self, tier, table_name):
        self.tier = tier
        self.table_name = table_name

    def read_xdb(self):
        xdb_data = None
        conn = None
        cur = None
        try:
            logger.info("Entering into method")
            locator = db_locator.Locator(tier_name=self.tier, role="scriptrw")
            locator.do_not_send_autocommit_query()
            conn = locator.create_connection(cursorclass=cursors.DictCursor)
            cur = conn.cursor()
            logger.debug("Reading xdb given table:{0} ".format(self.table_name))
            statement = "SELECT * FROM  " + self.table_name
            cur.execute(statement)
            xdb_data = cur.fetchall()
            # logger.debug("Rows read from table are {0}".format(xdb_data))

            logger.debug("Succesfully read from table {0}".format(self.table_name))
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

    def del_data(self):
        try:
            locator = db_locator.Locator(tier_name=self.tier, role="scriptrw")
            locator.do_not_send_autocommit_query()
            conn = locator.create_connection()
            cur = conn.cursor()
            del_stat = "DELETE FROM " + self.table_name
            cur.execute(del_stat)
            logger.debug("Deleted all the records successfully")
        except Exception as e:
            logger.exception(e)

    def write_xdb(self, xdbvalues):
        try:
            locator = db_locator.Locator(tier_name=self.tier, role="scriptrw")
            locator.do_not_send_autocommit_query()
            conn = locator.create_connection()
            cur = conn.cursor()

            cur.execute("SHOW columns FROM " + self.table_name)
            col_list = [column[0] for column in cur.fetchall()]
            if len(col_list) > 0:
                col_list = col_list[1:]
            else:
                logger.info("Table does not exist or may not have columns")
                exit(0)
            esc_seq_str = "%s," * len(col_list)
            listToStr = ",".join(map(str, col_list))
            insert_query = (
                "INSERT INTO "
                + self.table_name
                + "("
                + listToStr
                + ") "
                + "VALUES ("
                + esc_seq_str[: len(esc_seq_str) - 1]
                + ")"
            )
            logger.debug("Ready to write records")
            for i, data in enumerate(xdbvalues):
                try:
                    rowin = []
                    for key in data.keys():
                        if key != "id":
                            rowin.append(data[key])
                    cur.execute(
                        insert_query,
                        tuple(rowin),
                    )
                    logger.info("record %s created" % (i + 1))
                except Exception as ex:
                    logger.exception(ex)
                    continue
                except IndexError:
                    continue
                conn.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            conn.close()
            cur.close()


if __name__ == "__main__":
    Db = Db_Operations("", "")
    data = Db.read_xdb()
    Db.del_data()
    Db.write_xdb(data)
