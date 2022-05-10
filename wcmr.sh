#!/bin/bash

INPUT_PATH=$1
N=$2
M=$3
N_WORKERS=$4

mkdir -p ./files/intermediate/;
mkdir -p ./files/out/;
mkdir -p ./logs/;

python driver.py -N $N -M $M --input_path $INPUT_PATH --intermediate_path \
./files/intermediate/ --output_path ./files/out/ &
sleep 0.01;
for i in $(seq 1 $N_WORKERS); do python worker.py -i $i -s & done
