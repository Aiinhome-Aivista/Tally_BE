import sqlite3
import os

try:
    db_path = os.path.join('data', 'tally_sync_local.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("UPDATE mysql_config SET database_name = 'kidcity_solutions_pvt_ltd_25_26' WHERE database_name IS NULL OR database_name = ''")
    conn.commit()
    conn.close()
    print('Updated successfully!')
except Exception as e:
    print('Error:', e)
