#!/bin/sh

DATE=`date +%Y%m%d`
tar -czf pyramid.$DATE.linux-amd64.tar.gz -C cli/pyramid pyramid
