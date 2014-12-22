#!/Users/johnterzis/Library/Enthought/Canopy_64bit/User/bin/python

import sys

import numpy as np

import datetime as datetime
import pandas as pd

if '/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce' not in sys.path:
    sys.path.append('/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce')
    
import constants as con
'''
COLS = con.col_dict
CHUNKSIZE = con.CHUNK_SIZE
MAXLOOKBACK = con.MAX_LOOKBACK
MAXLOOKFORWARD = con.MAX_LOOKFORWARD
FEATURE_SYMBOLS = con.FEATURE_SYMBOLS
'''
#Do not run on files in Indicator folder. Only
#run on pre-merged files labeled with prefix 'merged_'


def test():
    pass

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    #a = CHUNKSIZE
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (word, 1)