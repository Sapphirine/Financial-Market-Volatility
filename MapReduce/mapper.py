#!/Users/johnterzis/Library/Enthought/Canopy_64bit/User/bin/python


import sys
if '/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce' not in sys.path:
    sys.path.append('/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce')
import constants as con
import datetime as datetime
import pandas as pd
import numpy as np
from utilities import writeToStdErr
'''
from lib2to3.pgen2.token import LESS
from numpy.oldnumeric.arrayfns import span
'''

COLS = con.col_dict
CHUNKSIZE = con.CHUNK_SIZE
MAXLOOKBACK = con.MAX_LOOKBACK
MAXLOOKFORWARD = con.MAX_LOOKFORWARD
FEATURE_SYMBOLS = con.FEATURE_SYMBOLS

first_row = False
#Do not run on files in Indicator folder. Only
#run on pre-merged files labeled with prefix 'merged_'


def generateKey(line,chunksize,maxlb,maxlf,chunk_ranges,date_ranges,
                file_start_dte):
    '''
    Given a list of strings composing one line from stdin, generate
    key that partitions file based on a chunksize, maxlookback and
    maxlookforward parameters
    
    key returned in form
    SYMBOL - STARTMIN OF CHUNK - ENDMIN OF CHUNK
    where startmin and endmin are elapsed min versus start date/time of 
    file
    '''
            
    time = line[COLS['Time']] 
    if len(time) == 3:
        new_time = time[0] + ':' + time[1:]

    elif len(time) == 4:
        new_time = time[:2] + ':' + time[2:]  
        
    line_dte = pd.to_datetime(line[COLS['Date']] + ' ' +
                              new_time,
                              infer_datetime_format = True)


    #number elapsed minutes of this line
    line_elapsed_min = divmod((line_dte - file_start_dte)
                              .total_seconds(),60)[0]        
    #create list of start and end times for each key
    '''
    num_chunks = np.floor(elapsed_minutes / (CHUNKSIZE * 24 
                                                * 60.0))
    '''
    '''                
    Note: since files might not have 24 hrs of minute data fully
    populated, number of observations for each key might be LESS
    than that implied by interval though value of key will span
    interval.
    '''
    count = 0
    for interv in chunk_ranges:
        #print 'checking interval {}'.format(count)
        start,end = date_ranges[count][0], date_ranges[count][1]
        if line_elapsed_min >= interv[0] and line_elapsed_min <= interv[1]:
            key = line[COLS['SYMBOL']] + '-' + str(start).replace('-',''). \
                replace(':','').replace(' ','') + '-' \
                + str(end).replace('-',''). \
                replace(':','').replace(' ','')
            return key
        count +=1
    #interval not found
    return None


def createIntervals(line, CHUNKSIZE, MAXLOOKBACK, MAXLOOKFORWARD):
    #format time in 09:30 format
    time_dict = {}
    for nm in ['STARTTIME','ENDTIME']:
        if len(line) >= COLS[nm]:
            time = line[COLS[nm]]
            if len(time) == 3:
                new_time = time[0] + ':' + time[1:]
                time_dict[nm] = new_time
            elif len(time) == 4:
                new_time = time[:2] + ':' + time[2:]
                time_dict[nm] = new_time
            else: 
                return None
            
    file_start_dte = pd.to_datetime(line[COLS['STARTDATE']] + ' ' +
                                time_dict['STARTTIME'],
                                infer_datetime_format=True)
    
    file_end_dte = pd.to_datetime(line[COLS['ENDDATE']] + ' ' +
                                  time_dict['ENDTIME'],
                                  infer_datetime_format = True)
    #number elapsed minutes in file
    elapsed_minutes = divmod((file_end_dte - file_start_dte)
                             .total_seconds(),60)[0]
    chunk_ranges,date_ranges = [],[]
    count = 0
    starttime, endtime = 0, CHUNKSIZE * 24 * 60.0
    start_date = file_start_dte 
    end_date = file_start_dte + datetime.timedelta(minutes=CHUNKSIZE * 24 * 60)
    while endtime < elapsed_minutes:
        #print 'Adding interval {}, endtime {},elapsed_min {}'. \
        #format(count,endtime,elapsed_minutes)
        #add begin and time from file start for each chunk
        chunk_ranges.append([starttime,endtime])
        date_ranges.append([start_date,end_date])
        start_date = start_date + datetime.timedelta(minutes = CHUNKSIZE * 24 * 60)
        end_date = end_date + datetime.timedelta(minutes = CHUNKSIZE * 24 * 60)
        starttime = starttime + (CHUNKSIZE * 24 * 60.0) 
        endtime = endtime + (CHUNKSIZE * 24 * 60.0)  
        count += 1
    #add stub date range for intervals smaller than chunksize
    date_ranges.append([start_date,file_end_dte])
    chunk_ranges.append([starttime,elapsed_minutes])
        
    return [chunk_ranges, date_ranges, file_start_dte]
    
    
intervals = []
file_start_dte = None
cnt, none_count, key_count = 0,0,0


for line in sys.stdin:

    #print 'original line {}'.format(line)
    line = line.strip()
    unpacked = line.split(",")
    
    if con.MAP_REDUCE == False:
        #print 'MapReduce is false'
        if cnt <= 0: #skip header
            cnt += 1
            continue
        elif cnt == 1: #create partition intervals of file once
            intervals, date_ranges, file_start_dte = createIntervals(
                        unpacked,CHUNKSIZE,MAXLOOKBACK,MAXLOOKFORWARD)
            cnt += 1
            continue 
        
        if len(intervals) < 0 or file_start_dte is None:
            #print 'interval or file start date is malformed! Skipping file'
            break
        
        #generate key per line
        key = generateKey(unpacked,CHUNKSIZE,MAXLOOKBACK,
                          MAXLOOKFORWARD,intervals,date_ranges,file_start_dte)
          
        if key is None:
            none_count += 1
            #print 'Consecutive None key #{}!'.format(none_count)
        else:
            none_count = 0
            key_count += 1           
            print '%s\t%s' % (key,unpacked)
            
    elif con.MAP_REDUCE == True:
        
        key_count += 1
        
        if key_count % 5000 == 0:
            writeToStdErr('Mapreduce: ')
            
        #try:
        if unpacked[0] == "Date": #skip header
            first_row = True
            continue           
        #repeat creation of intervals for each row since 
        #we do not know how mapreduce partitions multiple symbol 
        #files
        intervals, date_ranges, file_start_dte = createIntervals(
                unpacked, CHUNKSIZE, MAXLOOKBACK, MAXLOOKFORWARD)

        
        #generate key per line using date ranges of chunk
        key = generateKey(unpacked,CHUNKSIZE,MAXLOOKBACK,
                          MAXLOOKFORWARD,intervals,date_ranges,file_start_dte)
        
        if key is not None:
            print '%s\t%s' % (key,unpacked)
        #except:
        #    writeToStdErr('Error in Mapreduce: ')
        #    continue
        
    cnt += 1

