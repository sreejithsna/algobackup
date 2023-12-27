#!/bin/bash
echo " ############ WELCOME #################"

if [ -z "$1" ]
then
	echo " 		"  
	echo " Usage : edel_control.sh start/stop	" 
	echo " 		"  
	exit 1
fi
choice=$1
sh /home/opc/mydhan/DAEMONS.sh $choice