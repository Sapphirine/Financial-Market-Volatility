'''
utility functions for map reduce routine
'''
import constants as con
import sys
import os
COUNT = 1

def safeDFTextStore(path,df):
    #if file exists delete it
    try:
        if os.path.isfile(path):
          os.remove(path)  
          
        f = open(path, 'w')
        if df is not None:
            #write column names first
            f.write(str(con.reducer_cols)[1:-1] + '\n')
            for i in df.index:                   
                key_out = df.ix[i,'KEY']
                val_out = str(df.ix[i,1:].tolist())[1:-1]                        
                f.write(key_out + '\t' + val_out + '\n' )
        f.close()
    except IOError:
        sys.stderr.write('ioerror')
        sys.stderr.flush()
        return

def writeToStdErr(msg):
    
    #write msg suffixed by counter
    #and status template to notify
    #hadoop that were still alive!
    global COUNT
    COUNT = COUNT + 1
    if isinstance(msg,str):
        sys.stderr.write(msg + 'reporter:status:<msg>\n')
        sys.stderr.write('reporter:status:Hello!\n')
        sys.stderr.write('reporter:counter:MyGroup,MyCounter,{}\n'.format(COUNT))
        sys.stderr.flush()
    

def initDisplaceDict():
    displace_dict = {}
    for d in con.displacement:
        displace_dict[d] = {}
        displace_dict[d]['PLUS'] = {} #idx list/val list
        displace_dict[d]['MINUS'] = {}
        
        displace_dict[d]['PLUS']['idx'] = []
        displace_dict[d]['PLUS']['val'] = []
        
        displace_dict[d]['MINUS']['idx'] = []
        displace_dict[d]['MINUS']['val'] = []
    return displace_dict


#create time intervals
def perdelta(start, end, delta):
    curr = start
    while curr <= end:
        yield curr
        curr += delta
        
        
def convertFeatureToReturn(df,base_coln):
    '''
        df : dataframe of observations with displaced 
            closes for features populated. alter the displaced
            cols in form +,- xD by subtracting from their base close
            at that rows index time stamp which is also part of the row
        base_coln : base feature column name to operate on
    '''
    #remove rows that are not populated with closes.
    #resulting df is feature matrix in form f(x) = y
    #df = df.dropna(thresh=df.shape[1]-50)
    df = df[df.KEY != 'abc'].fillna(0)

    for base in base_coln:
        if base == 'SYMBOL':
            df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)].sub(
                           df.ix[:,'Close'],
                           axis = 0).abs().fillna(0)
            df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)].div(
                           df.ix[:,'Close'],
                           axis = 0)
            df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)].sub(
                           df.ix[:,'Close'],
                           axis = 0).abs().fillna(0)
            df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)].div(
                           df.ix[:,'Close'],
                           axis = 0)
        else:
            df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)].sub(
                           df.ix[:,filter(lambda x: base + '_CLOSE'
                           in x, con.reducer_cols)][base + '_CLOSE'],
                           axis = 0).abs().fillna(0)
            df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_+' 
                           in x,con.reducer_cols)].div(
                           df.ix[:,filter(lambda x: base + '_CLOSE'
                           in x, con.reducer_cols)][base + '_CLOSE'],
                           axis = 0)
            df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)].sub(
                           df.ix[:,filter(lambda x: base + '_CLOSE'
                           in x, con.reducer_cols)][base + '_CLOSE'],
                           axis = 0).abs().fillna(0)
            df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)] = \
                           df.ix[:,filter(lambda x: base + '_-' 
                           in x,con.reducer_cols)].div(
                           df.ix[:,filter(lambda x: base + '_CLOSE'
                           in x, con.reducer_cols)][base + '_CLOSE'],
                           axis = 0)     

    #DEBUG 12/10 remove cases where there are artificual 1's 
    #NOT indicative of actual vol.
    try:
        df = df[(df['SYMBOL_-1D']!=1) & (df['SYMBOL_+1D']!=1) & (df['SYMBOL_+7D']!=1) 
                & (df['SYMBOL_+20D']!=1) & (df['SYMBOL_-20D']!=1) &
                (df['SYMBOL_-7D']!=1) & (df['ES_-1D']!=1) & (df['VIX_-1D']!=1) 
                & (df['VIX_-7D']!=1)  & (df['VIX_-7D']!=1)  & (df['EURUSD_-7D']!=1)  &
                (df['XLE_-7D']!=1) ] 
    except KeyError:
        pass
    
    return df