#/usr/local/bin/python3.8
##############
## create a charge.db sqlite db or take the empty one from the repo.
# here is the create statement:
#-------------------------
# CREATE TABLE "charge" (
#         "id"    INTEGER,
#         "idTag" TEXT,
#         "meterStop"     INTEGER,
#         "reason"        TEXT,
#         "timestamp"     TEXT,
#         "transactionId" INTEGER,
#         "transactionData"       TEXT,
#         "unixtime"      INTEGER,
#         PRIMARY KEY("id")
# )
# CREATE UNIQUE INDEX "id" ON "charge" (
#        "id"
# )
#-------------------------


import sqlite3
import datetime
import logging
import requests
import csv
import urllib3

# we're talking to a local IP. IPs can't have valid TLS certs
# it breaks my security heart, but we ignore the TLS error.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# prepare out logging
logging.basicConfig(
     level=logging.INFO,
     format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )

logging.info("Begin fetching charging log")

payload = {"username": "admin",
           "password": "<PUK>"}

url_login = "https://192.168.1.248/cgi_c_login"
url_getCSV = "https://192.168.1.248/cgi_c_ldp1.session-charge_session"

# switch to make the script not pull the latest CSV and instead read from a
# local file called "tmp.csv". This is helpful if you want to just want to
# work on the sqlite import without permanently downloading the CSV
get_latest_data = True
connection_chargedb = sqlite3.connect('charge.db')
cursor_chargedb = connection_chargedb.cursor()

if __name__ == '__main__':
    if get_latest_data:
        # first login to the wallbox and retrieve the cookie
        try:
            logging.info("logging in to " + url_login)
            r = requests.post(url_login, data=payload, verify=False)
        except Exception as e:
            logging.error("Exception during login", exc_info=True)

        # download the csv and store it locally as tmp.csv
        try:
            logging.info("retrieving charging log from " + url_getCSV)
            init_dl = requests.get(url_getCSV, verify=False, cookies=r.cookies.get_dict(), )
            with open("tmp.csv", "wb") as f:
                f.write(init_dl.content)
            f.close()
        except Exception as x:
            logging.error("Exception during CSV DL", exc_info=True)


    # read the csv
    with open("tmp.csv",newline="\n") as csv_file:
        charge_data = csv.reader(csv_file, delimiter=";", quotechar='"')

        # jump over the first row, which is just the header
        for row in list(charge_data)[1:]:
            # before we add the transaction to the sqlite, we make sure, that its not already there
            logging.info("checking if transaction is already in chargeDB: " + row[7])
            cursor_chargedb.execute("select id from charge where transactionId='" + row[7] + "'")
            counter = cursor_chargedb.fetchone()

            if counter is None:
                logging.info("transactionID is not in ChargeDB - adding it: " + row[7])
                # calculate the total kwh for the transaction
                stop_value=float(row[6][:-4])
                start_value=float(row[5][:-4])
                meterStop = str(stop_value-start_value)
                if "RFID" in row[4]:
                    reason = "RFIDLogoff"
                else:
                    reason = "EVDisconnect"

                # write the data in the sqlite. I added the unixtimestamp here as well
                # to save me some time later in converting it
                timestamp = datetime.datetime.strptime(row[1], "%d-%m-%Y %H:%M:%S")
                transaction_tuple = (row[3], meterStop, reason, str(timestamp.isoformat()), "", row[7], str(int(timestamp.timestamp())))
                cursor_chargedb.execute(
                    '''INSERT into charge(idTag,meterStop,reason,timestamp,transactionData,transactionId,unixtime) VALUES(?,?,?,?,?,?,?)''',
                    transaction_tuple)
                connection_chargedb.commit()
            else:
                # looks like the charging event was already in the sqlite.
                logging.warning("not writing to chargeDB. We have a double transaction with transaction id: " + row[7])


logging.info("End fetching charging log")