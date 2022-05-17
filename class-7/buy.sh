#!/bin/bash

echo "start buying..."

count=0
while :
do
	mysql --defaults-extra-file=./mysql-config.cnf -Dmydb -e "insert into orders values(default, now(), 'free6om', 1024, 102, 0)"
	let count++
	if ! (( count % 10 )); then
		let "batch = count/10"
		echo $batch": got 10 products, gave 1024￥"
	fi
	sleep 0.05
done
