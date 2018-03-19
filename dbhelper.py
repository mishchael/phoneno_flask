#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Date    : 2018-03-11 14:29:11
# @Author  : Michael (mishchael@gmail.com)

import pymysql

def db_insert(sql):
	# 打开数据库连接
	db = pymysql.connect(host = '127.0.0.1', user = 'root', password = 'nlcyzqzy.0', db = 'rzdb', port = 3306, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
	# 使用 cursor() 方法创建一个游标对象 cursor
	cursor = db.cursor()
	# 使用 execute()  方法执行 SQL 插入语句
	try:
		# 执行sql语句
		cursor.execute(sql)
		# 提交到数据库执行
		db.commit()
		# print("**********数据插入成功**********")
	except:
		# 如果发生错误则回滚
		db.rollback()
	finally:
		# 关闭数据库连接
		cursor.close()
		db.close()


	'''
	数据库查询操作
	返回查询结果，list格式
	'''
def db_select(sql):
	# 打开数据库连接
	db = pymysql.connect(host = '127.0.0.1', user = 'root', password = 'nlcyzqzy.0', db = 'rzdb', port = 3306, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
	 
	# 使用cursor()方法获取操作游标 
	cursor = db.cursor()
	 
	# SQL 查询语句
	# sql = "SELECT * FROM bus_ydcj_power_consumption"
	       
	try:
		# 执行SQL语句
		cursor.execute(sql)
		# 获取所有记录列表
		results = cursor.fetchall()
		# for row in results:
		# 	print(row[2])

		# for row in results:
		#    fname = row[0]
		#    lname = row[1]
		#    age = row[2]
		#    sex = row[3]
		#    income = row[4]
		#     # 打印结果
		#    print ("fname=%s,lname=%s,age=%d,sex=%s,income=%d" % \
		#           (fname, lname, age, sex, income ))
	except:
		print ("Error: unable to fetch data")
	finally:
		# print(results)
		return results
		# 关闭数据库连接
		db.close()