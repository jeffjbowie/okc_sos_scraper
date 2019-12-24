#! /usr/bin/python3

"""
Oklahoma Secretary Of State Business Search

* Asterisk is allowed as a search term. Form validation fails to prevent submit button.
* No rate-limiting  HTTP requests
* Incremental filing #s -- easy to brute. UUIDs would be more efficient.

14,945 Records Total
1 Record / Sec.
4:15 to search through all records ?

SQLite DB Structure:

CREATE TABLE "companies" (
`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
`filing_number` INTEGER NOT NULL UNIQUE,
`established_date` TEXT,
`name` TEXT NOT NULL,
`address` TEXT NOT NULL,
`city_state_zip` TEXT NOT NULL,
`status` TEXT NOT NULL,
`registered_agent` TEXT NOT NULL
 )
"""

from requests import get
from bs4 import BeautifulSoup

# regular expressions, time, and SQLite libraries
import re
import time
import datetime
import sqlite3


# Drop into interactive shell @ any point in the code.
# import pdb
# pdb.set_trace()  # drop into interactive python shell


# Pull  details from SOS.OK.GOV business listings by filing #.
def scrape_company_data(filing_number):

    # Replace {0} with company_id argument
    data_url = 'https://www.sos.ok.gov/corp/corpInformation.aspx?id={0}'.format(filing_number)

    # Get raw HTML
    raw_html = get(data_url)

    # If 200 OK , proceed with scrape / process / DB insert.
    if raw_html.status_code == 200:

        html = BeautifulSoup(raw_html.text, 'html.parser')
        table = html.find("div", {"id": "printDiv"})

        company_name = (table.find('h3').get_text()).strip()

        if not company_name:
            return False

        ftable = table.find_all("dl", {"class": "ftable"})

        if not ftable:
            return False

        details_dds = ftable[0].find_all('dd')

        if not details_dds:
            return False

        information_dds = ftable[1].find_all('dd')

        if not information_dds:
            return False

        registered_agent = information_dds[0].get_text().strip('\r\n').strip()
        registered_agent = re.sub('\r\n', ' ', re.sub('\r\n', '', registered_agent))
        #registered_agent = re.sub('\xa0', '', registered_agent)
        registered_agent = re.sub('\s{2,}', ' ', registered_agent)

        if not registered_agent:
            registered_agent = ""

        address = information_dds[2].get_text().strip('\r\n').strip()

        if not address:
            return False
        else:
            address = re.sub('\s+', ' ', re.match('[A-Z0-9\w\s]*', address.replace("\n", "")).group())
            address = re.sub('\s{2,}', ' ', re.sub('\r\n', '', address))

        csz_string = information_dds[3].get_text().strip('\r\n').strip()

        if not csz_string:
            return False

        if "Active" not in details_dds[2].get_text().strip() or "In Existence" not in details_dds[2].get_text().strip():
            return False
        else:
            print(details_dds[2].get_text().strip())
            status = "Active"

        established_string = details_dds[5].get_text()

        established_year = established_string.strip()[-4:]
        established_month = re.search('([A-Za-z]{3})', established_string)
        established_day = established_string.replace(established_month.group(), "").replace(established_year, "")

        if not established_month and established_day and established_year:
            return False

        if int(established_year) < 2012:
            return False

        # City, State, Zip string
        csz_string = re.sub(' +', ' ', re.sub('[^0-9A-Za-z]', ' ', csz_string))

        zip_code = re.search('([0-9]{5})', csz_string)
        state = "OK"
        city = re.search('([A-Z ]*)(\sOK\s[0-9]{5})', csz_string)

        if not zip_code or not state or not city:
            return False

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S %p')

        sql = "INSERT INTO companies (filing_number, est_year, est_month, est_day, name, address, city, state, zip, status, registered_agent, created) \
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

        cur = conn.cursor()
        cur.execute(sql, (company_id, established_year, established_month.group(), established_day.strip(), company_name, address.strip("\n\r"), city.group(1), state, zip_code.group(), status, registered_agent, timestamp))
        conn.commit()

        print("COMPANY: %s EST DAY: %s EST MO: %s EST YR: %s ADDRESS: %s CITY: %s STATE: %s ZIP: %s AGENT:  %s STATUS: %s" % (company_name, established_day.strip(), established_month.group(), established_year, address, city.group(1), state, zip_code.group(), registered_agent, status))

    else:
        print("Error. Status Code: %s ; Try adjusting sleep quantity in while loop." % raw_html.status_code)


# Create our DB connection
conn = sqlite3.connect('companies.db')

# get lowest/highest filing # recorded in DB. Switch to DESC and rows  + 1 to go up.
sql = "SELECT filing_number FROM companies WHERE filing_number ORDER BY filing_number DESC LIMIT 1"
cur = conn.cursor()
cur.execute(sql)
rows = cur.fetchall()

# take last filing number. decrease if sorted ASC, increase if sorted DESC.
company_id = rows[0][0] + 1

# 10-digit company ID , start higher in hopes of reaching younger businesses.
#company_id = 3512692635

# brute force filing numbers.
while True:
    url = 'https://www.sos.ok.gov/corp/corpInformation.aspx?id={0}'.format(company_id)

    # build our list of IDs somehow , then loop through and process
    scrape_company_data(company_id)

    # Show current ID in case need to stop , and resume later. General indicator of progress.
    print("ID: %s \n\r" % company_id)

    # Increment if DESC /decrement IF ASC ID
    company_id += 1

    # Sleep for quarter of a second
    time.sleep(.250)


