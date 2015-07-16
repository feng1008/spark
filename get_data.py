#/usr/bin/python
# -*- coding:utf-8 -*-
import MySQLdb;
import codecs;

import sys;
reload(sys);
sys.setdefaultencoding('utf8');


def is_chinese(uchar):
        if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
                return True;
        else:
                return False;

CODING='utf-8';

try:
#     conn=MySQLdb.connect('localhost', 'root', '1008', 'feng');
    conn=MySQLdb.connect(host="localhost",user="root",passwd="1008",db="feng",charset="utf8");
    cursor = conn.cursor();
except:
    print "can not connect to the database!\n"
    exit();

# outputfile=codecs.open('/home/feng/com/data/data.csv', 'w', CODING);
outputfile=codecs.open('/home/feng/com/data/data.csv', 'a', CODING);

try:
    # sql='SELECT devices.imei, goods_types.title, purchases.buy FROM devices, goods_items, purchases, goods_sales, goods_types \
    #     WHERE purchases.sid = goods_sales.sid AND goods_sales.gid = goods_items.gid AND goods_items.tid = goods_types.tid AND devices.id=purchases.uid';
    sql='SELECT devices.imei, goods_types.title, purchases.buy, purchases.create_time FROM devices, goods_items, purchases, goods_sales, goods_types \
        WHERE purchases.sid = goods_sales.sid AND goods_sales.gid = goods_items.gid AND goods_items.tid = goods_types.tid AND devices.id=purchases.uid';
    cursor.execute(sql);
except:
    print "fail to get data from database!";

# outputfile.write('userId,itemNames,buys\n');

data=cursor.fetchall();
line_count=1;
for rows in data:
    for i in range(len(rows)):
        outputfile.write(str(rows[i]));
        if(i!=len(rows)-1):
            outputfile.write(",");
    outputfile.write("\n");
    print "writing file data.csv "+str(line_count)+" lines!";
    line_count+=1;
outputfile.close();

cursor.close();
