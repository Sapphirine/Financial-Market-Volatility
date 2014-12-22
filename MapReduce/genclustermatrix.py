#!/Users/johnterzis/Library/Enthought/Canopy_64bit/User/bin/python


'''
Create cluster feature matrix for a given symbol
as specified by txt file. 

Call as :

python genclustermatrix.py <file_path> <num_cols>

num_cols: maximum number of columns per row (30min time interval)
'''

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta


count = 0
PATH = '/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce' \
       '/data/'
OUT_FILE = "cluster_matrix.txt"

TARGET_COL = 'SYMBOL_+60M'

def keyfunc(timestamp, interval = 30*60):
    # defined a key function.
    # 1. parse the datetime string to datetime object
    # 2. count the time delta (seconds)
    # 3. divided the time delta with interval, which is (6*60) here
    xt = datetime(2013, 4,3)
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    except TypeError:
        print 'Type Error number {}'.format(timestamp)
        return time()
    delta_second = int((dt - xt).total_seconds())
    normalize_second = (delta_second / interval) * interval
    time_interval = xt + timedelta(seconds=normalize_second)
    return time_interval.time()


for arg in sys.argv:
    print arg
    
if len(sys.argv) == 3:    
    path = sys.argv[1]
    num_cols = sys.argv[2]
    raw_data = pd.read_csv(file_path)
    raw_data.rename(columns = lambda x: x.replace(' ','')[1:-1], inplace=True)
else:
    path = PATH
    for file in os.listdir(path):
        print 'Working on file {}'.format(file)
        if file[-3:] == 'txt':
            raw_data = pd.read_csv(path + file)
            raw_data.rename(columns = lambda x: x.replace(' ','')[1:-1], 
                            inplace=True)
            raw_data.Date = raw_data.Date.apply(lambda x: 
                                                str(x[11:-2]))
            raw_data['INTERVAL'] = raw_data.Date.apply(keyfunc)
            interv_grps = raw_data.groupby('INTERVAL')
            idx_list = []
            value_list = []
            min_list = 1000000
            try:
                for g in interv_grps:
                    time_interval = str(g[0])
                    df = g[1].sort(columns='Date',ascending=True,
                                   inplace=False)
                    val = df['SYMBOL_+60M'].values.tolist()
                    if np.median(val) ==1:
                        continue
                    idx_list.append(time_interval)
                    len_val = len(val)
                    if len_val < min_list:
                        min_list = len_val
                    value_list.append(val)
                
                trunc_val_list = []
                for list in value_list:
                    if len(list) > min_list:
                        trunc_val_list.append(list[:min_list])
                    else:
                        trunc_val_list.append(list)
                
                cluster_matrix = pd.DataFrame(trunc_val_list, index=idx_list, 
                                              columns = np.arange(min_list))
            except:
                print 'Problem encountered with file {}'.format(file)
                continue
            
            out_file = PATH + file[:3] + OUT_FILE    
            cluster_matrix.to_csv(out_file)    
                
                
            

    
    
    
    
    