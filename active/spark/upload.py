#!/usr/bin/python
#-*- coding:utf8 -*-
import os

def main():
	for lines in open('catename_cnen.txt', 'r').readlines():
		category=lines.strip('\r\n').split('\t')[1]
		os.system('aws s3 mb s3://datamining.ym/user_profile/'+category)

if __name__=='__main__':
	main()
