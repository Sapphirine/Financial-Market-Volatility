#!/Users/johnterzis/Library/Enthought/Canopy_64bit/User/bin/python

'''
ADD MISSING comma to first column
'''

import os 
import sys

pth='/Users/johnterzis/projects/source/MarketVol'\
'/Preprocessor/MapReduce/data/'

for file in os.listdir(pth):
    if file[-3:] =='txt':
        print 'Amending file {}'.format(file)
    with open(pth + file,'r+') as f:
        lines = f.readlines()
        f.seek(0)
        f.truncate()
        first_line = True
        for line in lines:
            try:
                if first_line == False:
                    
                    new_line = line.split('\t')[0] + ',' +line.split('\t')[1]
                    f.write(new_line)
                else:
                    first_line = False
                    new_line = line
                    f.write(new_line)
            except IndexError:
                print 'Offending file {}'.format(file)
                break
