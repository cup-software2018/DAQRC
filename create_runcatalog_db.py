import sys
import os
import getopt
import sqlite3


def help(name):
    mess = '\n'
    mess += '  Usage: %s [OPTIONS] ...' % name
    mess += '\n'
    mess += '  OPTIONS:\n'
    mess += '    -a         add AMOREADCDAQ\n'
    mess += '    -f         add FADCDAQ\n'
    mess += '    -s         add SADCDAQ\n'
    mess += '    -i         add IADCDAQ\n'
    mess += '    -o FILE    database file\n'
    print(mess)


def main():
    if len(sys.argv) == 1:
        help(sys.argv[0])
        sys.exit(1)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "afsio:")
    except getopt.GetoptError as err:
        print("%s" % err)
        sys.exit(1)

    dbfile = 'runcatalog.db'

    # Initialize DAQ flags
    DAQS = ["namoreadc", "nfadc", "nsadc", "niadc"]
    enabled = {daq: False for daq in DAQS}

    for opt, arg in opts:
        if opt == '-a':
            enabled["namoreadc"] = True
        elif opt == '-f':
            enabled["nfadc"] = True
        elif opt == '-s':
            enabled["nsadc"] = True
        elif opt == '-i':
            enabled["niadc"] = True
        elif opt == '-o':
            dbfile = arg

    if os.path.isfile(dbfile):
        print('%s already existed, exit ...' % dbfile)
        sys.exit(1)

    # 1. Build the base SQL CREATE TABLE query string
    create_query = """
    CREATE TABLE runcatalog (
        runnum INTEGER PRIMARY KEY AUTOINCREMENT,
        runtype TEXT,
        rundesc TEXT,
        shift TEXT,
        config TEXT,
        stime TEXT,
        etime TEXT,
        onlbit INTEGER,
        offbit INTEGER,
        runlog TEXT
    """

    # 2. Append dynamic columns based on enabled DAQs
    for daqname in DAQS:
        if enabled[daqname]:
            create_query += f",\n        {daqname} INTEGER"
            tname = daqname.replace('n', 't')
            create_query += f",\n        {tname} REAL"

    # Close the query string
    create_query += "\n    );"

    # 3. Connect to SQLite and execute the query
    # This automatically creates the file if it doesn't exist
    conn = sqlite3.connect(dbfile)
    cursor = conn.cursor()

    cursor.execute(create_query)
    conn.commit()

    # 4. Print table info to verify the schema
    print("Database initialized. Columns in 'runcatalog':")
    cursor.execute("PRAGMA table_info(runcatalog);")
    columns = cursor.fetchall()

    for col in columns:
        # col[1] is column name, col[2] is data type
        print(f"  - {col[1]} ({col[2]})")

    # Close connection
    conn.close()


if __name__ == '__main__':
    main()
