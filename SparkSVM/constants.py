'''
Store initialization constants here
'''
HDF = False

LOCAL=True


DFLT_VOL = '7' #by default predict 7 day vol

DATA_PATH = '/Users/johnterzis/projects/source/MarketVol' \
            '/Preprocessor/MapReduce/data/' 

FILE_NM = 'AIG-20081228093200-20111228093200.txt'

PROCESSED_FN = 'mllib_input_' + FILE_NM[:2] + '.txt'

TRAIN_FN =  'mllib_train_' + FILE_NM[:2] + '.txt'

TEST_FN =  'mllib_test_' + FILE_NM[:2] + '.txt'

#columns to use from feature matrices for regressor
INPUT_COL_KEEP = ['SYMBOL_-60M','SYMBOL_-3D',
              'SYMBOL_-7D','ES_-20D','VIX_-20D','ES_-1D','ES_-7D',
              'VIX_-1D','VIX_-7D','EURUSD_-7D','XLE_-7D']

OUTPUT_COL_KEEP = ['SYMBOL_+1D','SYMBOL_+7D','SYMBOL_+20D']


