#!/bin/bash
IFS=$'\r\n' 
read -d '' -r -a lines < $1
#read in file provided as input to the script. i.e. ./test.sh asdf.csv
echo Process factors into Volatility of a Share
for x in "${lines[@]}"
do
mahout trainlogistic --input /Volumes/Data/BigData/FinalProject/cluster/$2 --output ./model$x --target Volatile --types numeric --predictors $x  --categories 2 --rate 1
mahout runlogistic --input /Volumes/Data/BigData/FinalProject/cluster/$2 --model ./model$x --auc --confusion
echo $x Training was run on dataset $2
done