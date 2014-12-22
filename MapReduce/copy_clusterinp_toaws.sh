#!/bin/bash

source ~/.bashrc

MARKET_VOL=/Users/johnterzis/projects/source/MarketVol
OUT_FILES=$MARKET_VOL/Preprocessor/MapReduce/data/*cluster*
KEYGEN=/Users/johnterzis/.ssh/AWS-HDPCluster-Keypair1.pem

for f in $OUT_FILES;
do
    echo "Processing $f file..."
    #use rsync to gracefully reconnect on interrupts and xfer difference only
    rsync -avz -e "ssh -i $KEYGEN" $OUT_FILES \
    ec2-user@54.148.236.198:/home/ec2-user/Feature_Data/
done