#!/bin/bash
command='1'
while [ $command != '0' ]
do
    scrapy crawl cloud2 --nolog
    command=$(cat $(dirname $0)/control.txt)
done
