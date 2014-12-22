#!/bin/bash

source ~/.bashrc


MARKET_VOL=/Users/johnterzis/projects/source/MarketVol
MAPREDUCE_CODE=$MARKET_VOL/Preprocessor/MapReduce
MERGED_FILES=$MARKET_VOL/Preprocessor/data/AllSymbols/Merged_Data/*.txt

for f in $MERGED_FILES;
do
    echo "Processing $f file..."
    #pipe last 1.5mm rows of each file to map reduce locally
    tail -n 1500000 $f | $MAPREDUCE_CODE/mapper.py | $MAPREDUCE_CODE/reducer.py
done