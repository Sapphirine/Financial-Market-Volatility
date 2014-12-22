import numpy as np
import os 

MAP_REDUCE = True

OUTPUT_H5 = os.path.abspath(os.curdir) + '/' + 'reduced_out.h5' 

#size of each map value in days of observations
CHUNK_SIZE = 365*.75

#max past periods in days needed to create input rows
MAX_LOOKBACK = np.min([CHUNK_SIZE,30*3])
MAX_LOOKFORWARD = np.min([CHUNK_SIZE,30*3])

columns = ['Date','Time','Open','High','Low',
           'Close','Volume','SYMBOL','ENDDATE',
           'STARTDATE','STARTTIME','ENDTIME','INDEX']

displacement = ['60M','1D','3D','7D','20D','40D','90D'] 

#after merging features
merged_cols = ['Date','Time','Open','High','Low','Close','Volume','SYMBOL','ENDDATE','STARTDATE','STARTTIME',
               'ENDTIME','INDEX','ADV_CLOSE','DECL_CLOSE','DVOL_CLOSE','DXY_CLOSE','ES_CLOSE',
               'EURUSD_CLOSE','GLD_CLOSE','INDU_CLOSE','NDX_CLOSE','OEX_CLOSE','PREM_CLOSE',
               'QQQ_CLOSE','SPX_CLOSE','TICK_CLOSE','TIKI_CLOSE','TRIN_CLOSE','UVOL_CLOSE',
               'VIX_CLOSE','XLE_CLOSE','XLF_CLOSE']

#reducer final cols for output
tmp = [col[:-6] for col in merged_cols[13:]] + ['SYMBOL']
tmp_final = []
for interv in displacement:
    for ind in tmp:
        tmp_final.append(ind + '_+' + interv)
        tmp_final.append(ind + '_-' + interv)
    
reducer_cols = ['KEY'] + merged_cols + tmp_final

col_dict = {}

idx = 0
for col in columns:
    col_dict[col] = idx
    idx = idx + 1
    

FEATURE_SYMBOLS = ['TRIN','VIX','ES']
    
    