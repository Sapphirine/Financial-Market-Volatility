'''
Inner Join feature symbols with dependent symbols
by timestamp

Must know path to feature symbol .txt files 
and dependent symbol .txt files
'''
import constants as con

import os
import pandas as pd
import sys



#feature symbols must be in Indicator path
PATH_FEATURES = '/Users/johnterzis/projects/source/MarketVol' \
'/Preprocessor/data/AllSymbols/Indicators'

PATH_INPUTS = '/Users/johnterzis/projects/source/MarketVol' \
'/Preprocessor/data'


'''
Walk data path for text files and add metadata columns 
overwriting original data
'''

count = 0
for root,dirs,files in os.walk(PATH_INPUTS):
    if 'Indicators' not in root.split('/'):
        for file in files:
            if file[-3:] == 'txt' and file[:7] != 'merged_':
                print 'Processing file {}, number {}'.format(files,count)
                count = count + 1
                df = pd.read_csv(root + '/' + file)
                for root1,dirs1,files1 in os.walk(PATH_FEATURES):
                    for file1 in files1:
                        if file1[-3:] == 'txt':
                            print 'merging file {} with input file {}'.format(
                                                file,file1)
                            feat_df = pd.read_csv(root1 + '/' + file1)
                            feat_symbol = feat_df.irow(0).SYMBOL
                            new_col = feat_symbol + '_CLOSE'
                            feat_df[new_col] = feat_df['Close']
                            if new_col not in df.columns.tolist():
                                df = pd.merge(df,feat_df[['Date', 'Time',
                                                                new_col]], how = 'left',
                                                     on=['Date','Time'])
                df.to_csv(root + '/' + 'merged_' + file, index = False)
