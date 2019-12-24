import re
import sqlite3
import pdb

# Create our DB connection
#conn = sqlite3.connect('companies.db')

# get lowest/highest filing # recorded in DB. Switch to DESC and rows  + 1 to go up.
sql = "SELECT id,registered_agent FROM companies"
cur = conn.cursor()
cur.execute(sql)
rows = cur.fetchall()

for row in rows:

    print(row)

    if "\n" in row[1] or "\r" in row[1]:



        sanitized = re.sub('\r\n', ' ', re.sub('\r\n', '', row[1]))
        sanitized = re.sub('\xa0', '', sanitized)
        sanitized = re.sub('\s{2,}', ' ', sanitized)

        print(sanitized)

        #sql = "UPDATE companies SET registered_agent = ? WHERE id = ?"
        #cur.execute(sql, (sanitized, row[0]))
        #conn.commit()

