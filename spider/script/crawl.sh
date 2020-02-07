#!/bin/bash
command=$1
if [ -z "$command" ] || [ $command = "start" ];then
    echo 1 > $(dirname $0)/control.txt
    nohup sh $(dirname $0)/cloud.sh >$(dirname $0)/log.txt 2>&1 &
    echo 'started'
elif [ $command == "stop" ];then
    echo 0 > $(dirname $0)/control.txt
    echo 'stopped'
else
    echo "wrong command (use start, stop)"
fi
