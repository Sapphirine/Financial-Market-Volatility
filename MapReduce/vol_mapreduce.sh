#!/bin/bash

source ~/.bashrc
export TEST_FILE=/Users/johnterzis/projects/source/MarketVol/Preprocessor/data/AllSymbols/Stocks/AA/merged_AA.txt
export MAPREDUCE_CODE=/Users/johnterzis/projects/source/MarketVol/Preprocessor/MapReduce

if [ "$MR_HADOOP" == "True" ] 
then
    if [ "$NO_REDUCE"  == "True" ]
    then
        echo "Running just Mapper!"
        
        $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/libexec/share/hadoop/tools/lib/hadoop-streaming-2.5.2.jar \
        -D mapred.reduce.tasks=0 -file $MAPREDUCE_CODE/mapper.py -mapper $MAPREDUCE_CODE/mapper.py -file $MAPREDUCE_CODE/reducer.py \
        -reducer $MAPREDUCE_CODE/reducer.py -input /bigdata/data/merged_AA.txt \
        -jobconf stream.non.zero.exit.is.failure=false -output /bigdata/mapreduce/output17 \
        -numReduceTasks 0 
    else 
        $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/libexec/share/hadoop/tools/lib/hadoop-streaming-2.5.2.jar \
        -file $MAPREDUCE_CODE/mapper.py -file $MAPREDUCE_CODE/reducer.py  -mapper $MAPREDUCE_CODE/mapper.py \
        -reducer $MAPREDUCE_CODE/reducer.py -input /bigdata/data/merged_AA.txt \
        -jobconf stream.non.zero.exit.is.failure=false -output /bigdata/mapreduce/output17     
    fi
else
    tail -n 200000 $TEST_FILE | python $MAPREDUCE_CODE/mapper.py | python $MAPREDUCE_CODE/reducer.py
fi