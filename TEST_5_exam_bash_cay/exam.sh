#!/bin/bash

func1 () {
sales=$(curl -X GET http://0.0.0.0:5000/$1)
echo "$1 : $sales" >> sales.txt
}

my_array=(rtx3060  rtx3070  rtx3080  rtx3090  rx6700)

date >> sales.txt
for i in ${my_array[*]}
	do
	func1 $i
	done
