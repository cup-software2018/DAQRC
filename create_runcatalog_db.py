#!/usr/bin/env python3

import sys
import os
import getopt
from pydblite.sqlite import Database, Table


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

    DAQS = ["naadc", "nfadc", "nsadc", "niadc"]
    enabled = {}
    for daqname in DAQS:
        enabled[daqname] = False

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

    db = Database(dbfile)
    table = Table('runcatalog', db)

    table.create(('runnum', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
                 ('runtype', 'TEXT'),
                 ('rundesc', 'TEXT'),
                 ('shift', 'TEXT'),
                 ('config', 'TEXT'),
                 ('stime', 'TEXT'),
                 ('etime', 'TEXT'),
                 ('onlbit', 'INTEGER'),
                 ('offbit', 'INTEGER'),
                 ('runlog', 'TEXT'))

    for daqname in DAQS:
        if enabled[daqname]:
            table.add_field(daqname, 'INTEGER')
            tname = daqname.replace('n', 't')
            table.add_field(tname, 'REAL')

    for item in table.info():
        print(item)


if __name__ == '__main__':
    main()
