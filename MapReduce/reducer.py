#!/Users/johnterzis/Library/Enthought/Canopy_64bit/User/bin/python


from operator import itemgetter
import sys
if '/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce' not in sys.path:
    sys.path.append('/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce')
import constants as con
import pandas as pd
from datetime import date, datetime, timedelta
import numpy as np
from utilities import *

key = None
current_count = 0
current_key = None
start, end = None, None
df= pd.DataFrame()
idx,values, cnt_mm = [],[],[]
cnt_mm.append(0)
key_count = 0

#various temp containers to populate df per key
#one list of vals_plus/minus, idx_plus/minus for each 
#displacement column
displace_dict = initDisplaceDict()


    
def populateIndices(df,value_list, key, start,end):
    '''
    For every row that comes in stdin,
    populate other rows of df based on lookback
    or lookforward implied by destination df
    '''
    #print 'populateIndices with base date {}, df shape {}'.format(value_list[0],df.shape)
    #print 'key {} value list {}'.format(key,value_list[:13])
    dte = value_list[0]
    #add closes for features and symbol for that timestamp first
    idx.append(dte)
    values.append([key] + value_list)
    '''
        if dte in df.index.tolist():
            df.ix[dte,:14] = [key] + value_list[:13]
    '''       
    #add feature closes for displaced timestamps
    for displace in con.displacement:
        feature_vals = value_list[13:] + [value_list[5]]
        length_val_list = len(feature_vals)
        
        if displace[-1] == 'M':
            idx_plus = dte+timedelta(minutes=int(displace[:-1]))
            idx_minus = dte-timedelta(minutes=int(displace[:-1]))
            
            if length_val_list != len(con.tmp):
                continue
            
            if idx_plus <= end: 
                displace_dict[displace]['PLUS']['idx'].append(idx_plus)
                displace_dict[displace]['PLUS']['val'].append(feature_vals)
            if idx_minus >= start:
                displace_dict[displace]['MINUS']['idx'].append(idx_minus)
                displace_dict[displace]['MINUS']['val'].append(feature_vals)


        elif displace[-1] == 'D':
            idx_plus = dte + timedelta(days=int(displace[:-1]))
            idx_minus = dte - timedelta(days=int(displace[:-1]))

            
            if length_val_list != len(con.tmp):
                continue
            if idx_plus <= end: 
                displace_dict[displace]['PLUS']['idx'].append(idx_plus)
                displace_dict[displace]['PLUS']['val'].append(feature_vals)
            if idx_minus >= start:
                displace_dict[displace]['MINUS']['idx'].append(idx_minus)
                displace_dict[displace]['MINUS']['val'].append(feature_vals)


    


# input comes from STDIN
for line in sys.stdin:
    #remove leading and trailing whitespace
    try:
        line = line.strip()   
        #key value pair come in each as strings
    
        key, value = line.split('\t')    
        value_list = value.replace('[','').replace(']','').split(',')
        value_dict, count = {}, 0
        time = value_list[1].replace("'","").replace(' ','')
    
         
        for j in value_list:
            j_stripped = j.replace("'","").replace(' ','')
    
            if count == 0 :
                #format date to datetime with time down to minute
                if len(time) == 3:
                    time = '0' + time            
                dte = pd.to_datetime(j_stripped + ' ' + time,
                                    infer_datetime_format=True)
    
                value_list[count]= dte
            elif count  >= 7 and count <= 12:
                value_list[count] = j_stripped
            elif count != 1:
                if len(j_stripped) == 0:
                    value_list[count] = 0
                else:
                    value_list[count] = float(j_stripped)
            count += 1
        #print '{}, {}'.format(key,value_list)
    except:
        writeToStdErr('Reducer:error in line split')
        
    key_count += 1
    if key_count % 5000 == 0:
    #increment counter and update status so hadoop does not timeout
        writeToStdErr('Reducer: About to pop indices')

    
    if key == current_key:
        #populate the right indices with closing prices
        try:
            populateIndices(df, value_list, key, start,end)
        except:
            writeToStdErr('Reducer:error in populate indices')
    else: #on key switches, build output df for existing key at once
        #print 'REDUCER: SHAPE OF DF {}'.format(df.shape[0])
        try:
            if not df.empty:
                #print 'LENGTH_VAL_LIST TOTAL MISMATCH FROM REST NUMBER {}'.format(cnt_mm[0])
                
                #populate df at once using stored indices and values
                df.ix[idx,:34] = values #close values of timestamp
                
                for d_key in displace_dict:
                    writeToStdErr('Reducer: about to pop df')
    
                    dd = displace_dict
                    #if indices are empty, do not populate
                    minus_idx = dd[d_key]['MINUS']['idx']
                    plus_idx = dd[d_key]['PLUS']['idx']
                    if con.MAP_REDUCE == False:
                        print 'inserting vals for displacement cols {}'.format(d_key)
    
                    #go backwards in index vs closing timestamp and populate a forward looking + column
                    if len(minus_idx) > 0:
                        df.ix[ minus_idx, filter(lambda x: '+' + d_key
                                            in x,con.reducer_cols)] = dd[d_key]['MINUS']['val']
                    #vice versa                    
                    if len(plus_idx) > 0:              
                        df.ix[ plus_idx, filter(lambda x: '-' + d_key
                                       in x,con.reducer_cols)] = dd[d_key]['PLUS']['val']
        except:
            writeToStdErr('Reducer:error in populated vals')

        try:
            #convert displaced feature cols to absolute value of returns for base vs displaced time period by symbol
            df = convertFeatureToReturn(df, con.tmp)
        
            idx,values= [],[]     
            displace_dict = initDisplaceDict()
        except:
            writeToStdErr('Reducer:error in convertFeatures')
            
        #print dataframe line by line as key, value
        if con.MAP_REDUCE == True:
            try:
                if not df.empty:
                    idx_count = 0
                    for i in df.index:                   
                        idx_count += 1
                        if idx_count % 5000 == 0:
                            writeToStdErr('Reducer: about to write output')
                            #DEBUG PURPOSES ONLY
                            
                        key_out = df.ix[i,'KEY']
                        val_out = str(df.ix[i,1:].tolist())[1:-1]                        
                        print '{}\t{}' % (key_out, val_out)    
            except:
                writeToStdErr('Reducer:error in printing df to stdio')      
                     
        elif con.MAP_REDUCE == False:
            #print test output to h5 for debugging
            if not df.empty:
                str_key = str(df.irow(0)['KEY'])
                '''
                store = pd.HDFStore(con.OUTPUT_H5, complevel =8, complib='zlib')
                store[ str_key ] = df
                store.close()
                '''
                #store key, df in a text file as well
                file_path = os.path.abspath(os.curdir) + '/data/' + str_key + '.txt'
                safeDFTextStore(file_path, df)
                
        current_key = key
        
        #print 'Initiating new empty dataframe to store transformed vals'
        
        #re-init new df for time interval based on key's begin/end dates
        start = pd.to_datetime(key.split('-')[1],
                                   infer_datetime_format = True)
        end = pd.to_datetime(key.split('-')[2],
                                   infer_datetime_format = True)
        df = None
        df = pd.DataFrame(0,columns = con.reducer_cols,
                          index = [i for i in perdelta(start, end,
                                                       timedelta(minutes=1))])
        #ensure key column is a string
        df['KEY'] = 'abc'
        
        #print 'done creating tmp dataframe'
        
        

    