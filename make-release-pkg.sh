#!/bin/sh

DATE=`date +%Y%m%d`
tar -czf eventhorizon.$DATE.linux-amd64.tar.gz -C cli/horizon horizon
