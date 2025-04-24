
""" Imports """
import getopt
import sys
import datetime
from loader.loader import Loader

if __name__ == "__main__":

    short_options = "i:o:c:l:d"
    long_options = ["input=","output=","connection=","limit=","drop"]

    input = None
    output = None
    connection = None
    limit = 0
    drop = False
    suffix = str(datetime.datetime.today()).split()[0]

    arguments = sys.argv[1:]

    options, values = getopt.getopt(arguments, short_options, long_options)

    for o, v in options:
        if o == "-i":
            input = v
        if o == "-o":
            output = v
        if o == "-c":
            connection = v
        if o == "-l":
            limit = int(v)
        if o == "-d":
            drop = True

    reporter = Loader(input, output, connection, limit, drop)

    reporter.load()
