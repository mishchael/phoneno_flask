#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-03-15 18:20:31
# @Author  : mishchael


import json
from bs4 import BeautifulSoup
import pymysql
import dbhelper
import requests
import time
import datetime

#User-Agent信息
user_agent = r'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)'
#Headers信息
headers = {
	"Accept": "*/*",
	"Accept-Encoding": "gzip,deflate",
	"Accept-Language": "zh-CN",
	"Connection": "Keep-Alive",
	"Content-Type":"application/x-www-form-urlencoded",
	"User-Agent": user_agent,
	"Referer": "http://10.158.249.36/"
		}
#设置cookies
cookie_jar = requests.cookies.RequestsCookieJar()
logonUsername = '871014222XA'
JSESSIONID = '0w7NhqfdWjL68ffhLj1LxpJdXwnJ1zZJLB1VqV1KHRYLBlvlDw4W!-10388351'
cookie_jar.set('logonUsername', logonUsername, domain='10.158.249.36', path='/')
cookie_jar.set('JSESSIONID', JSESSIONID, domain='10.158.249.36', path='/')

'''
处理登录,更新cookie_jar
'''
def login():
	url_login = 'http://10.158.249.36/web/pf/authentication/logon.do'
	# url_kick = 'http://10.158.249.36/web/pf/authentication/eliminateUserDialog.do'
	#用户名和密码登录	
	form_data_login = {}
	form_data_login['kind'] = '1'
	form_data_login['clientIPAddr'] = '10.192.153.10'
	form_data_login['clientMacAddr'] = ''
	form_data_login['clientMachineName'] = ''
	form_data_login['isNeedRSA'] = 'Y'
	form_data_login['username'] = '871014222XA'
	form_data_login['password'] = '3020524cd838e1450827f12f57b155a2514e27eff92230333c44daf713d441bf286de00468f012dddb4c63da2001bac2e4d5c7d3d42a42a9c361085ef1235a5ec5e9eaccb72c68c11fc4bda405bab938c80d809a95b40ceed9f064ea15a595708663720c0d3a336f9e79f160fb43ca10186ba59a71f5e6dc964f756bfaa2ecdb'
	
	rsp_login = requests.post(url_login, headers = headers, cookies = cookie_jar, data = form_data_login)
	cookie_jar.set('JSESSIONID', rsp_login.cookies['JSESSIONID'], domain='10.158.249.36', path='/')


'''
查询超5户共用1个手机号码明细
返回dict，格式{'page_size':'','record_count':'','page_count':''}
'''
def query_page_count():
	url = 'http://10.158.249.36/web/sp/custquery/impl/lsCustQueryImpl.do'
	#form中有相同名称的变量，使用tuple list
	form_data = []
	form_data.append(('queryNo','SJH_CF5'))
	form_data.append(('queryName','超5户共用1个手机号码明细'))
	form_data.append(('stateFlag','1'))
	form_data.append(('advSearchFlag','N'))
	form_data.append(('exportDirect','N'))
	form_data.append(('autoQuery','N'))
	form_data.append(('exportType',''))
	form_data.append(('notAllowExport',''))
	form_data.append(('notautosubmit',''))
	form_data.append(('paraTypeCode','05'))
	form_data.append(('paraName','单位代码'))
	form_data.append(('paraValue','37411'))
	form_data.append(('paraTypeCode','日照供电公司'))
	form_data.append(('queryNo','01'))
	form_data.append(('paraName','抄表段编号'))
	form_data.append(('paraValue',''))
	form_data.append(('action','search'))

	headers['Referer'] = 'http://10.158.249.36/web/sp/custquery/impl/lsCustQueryImpl.do?action=impl&queryNo=SJH_CF5&stateFlag=1&queryName=%E8%B6%855%E6%88%B7%E5%85%B1%E7%94%A81%E4%B8%AA%E6%89%8B%E6%9C%BA%E5%8F%B7%E7%A0%81%E6%98%8E%E7%BB%86&U=1521115060211'
	resp = requests.post(url = url, data = form_data, headers = headers, cookies = cookie_jar)
	html = resp.text
	page_info_dict = {}
	try:
		soup = BeautifulSoup(html, 'lxml')
		html_pageinfo = soup.find('td', class_ = 'page-toolbar')
		#每页记录数，结果应该是50
		page_size = soup.find('span', id = 'custQueryResultsMaxRow').string
		page_info_dict['page_size'] = page_size
		#记录总数，共多少条结果
		record_count = soup.find('span', id = 'custQueryResultsRowCount').string
		page_info_dict['record_count'] = record_count
		#总页数
		page_count = int(int(record_count) // int(page_size)) + 1
		page_info_dict['page_count'] = page_count
	except Exception as e:
		print(e)
	finally:
		return page_info_dict
	

'''
查询超5户共用1个手机号码明细
返回liet，格式[{一条数据},{一条数据}]
'''
def query_1phone_for_5cons(pageno, pagesize):
	url = 'http://10.158.249.36/web/sp/custquery/impl/lsCustQueryImpl.do'
	#form中有相同名称的变量，使用tuple list
	form_data = []
	form_data.append(('queryNo', 'SJH_CF5'))
	form_data.append(('queryName', '超5户共用1个手机号码明细'))
	form_data.append(('advSearchFlag', 'N'))
	form_data.append(('exportType', ''))
	form_data.append(('notAllowExport', ''))
	form_data.append(('paraName', '单位代码'))
	form_data.append(('paraValue', '37411'))
	form_data.append(('paraTypeCode', '37411'))
	form_data.append(('paraName', '抄表段编号'))
	form_data.append(('paraValue', ''))
	form_data.append(('paraTypeCode', ''))
	form_data.append(('custQueryResults_sid', 'null'))
	form_data.append(('custQueryResults_colname', ''))
	form_data.append(('custQueryResults_sorttype', ''))
	form_data.append(('custQueryResults_pageno', pageno))
	form_data.append(('custQueryResults_pagesize', pagesize))
	form_data.append(('action', 'search'))
	
	headers['Referer'] = 'http://10.158.249.36/web/sp/custquery/impl/lsCustQueryImpl.do'
	resp = requests.post(url = url, data = form_data, headers = headers, cookies = cookie_jar)
	html = resp.text
	result_list = []
	try:
		soup = BeautifulSoup(html, 'lxml')
		html_pageinfo = soup.find('td', class_ = 'page-toolbar')
		#每页记录数，结果应该是50
		page_size = soup.find('span', id = 'custQueryResultsMaxRow').string
		#记录总数，共多少条结果
		record_count = soup.find('span', id = 'custQueryResultsRowCount').string
		#总页数
		page_count = int(int(record_count) // int(page_size)) + 1
		#获取第1页结果
		html_result = soup.find('table' , id = 'custQueryResultsBodyTable').find('tbody', id = 'custQueryResultsBody').find_all('tr')
		for tr in html_result:
			html_td = tr.find_all('td', field = True)
			row_dict = {}
			for td in html_td:
				field = td['field']
				if field == '单位代码':
					row_dict['orgno'] = td.string
				if field == '供电单位':
					row_dict['orgname'] = td.string
				if field == '户号':
					row_dict['consno'] = td.string
				if field == '户名':
					row_dict['consname'] = td.string
				if field == '手机号':
					row_dict['consphone'] = td.string
				if field == '用电地址':
					row_dict['consaddr'] = td.string
				if field == '抄表段编号':
					row_dict['cbdno'] = td.string
				if field == '抄表段名称':
					row_dict['cbdname'] = td.string
				if field == '统计时间':
					row_dict['datadate'] = td.string
			#判断是否取到数据，第一个td是没有数据的
			if len(row_dict) > 0:
				#如果供电单位是高兴供电所，跳过不处理
				#应为高兴供电所已合并到巨峰，但是系统内仍有数据，且巨峰也有数据，重复
				if row_dict['orgname'] == '高兴供电所':
					continue
				else:
					result_list.append(row_dict)

		# print(result_list)
		# print(len(result_list))
		# print(page_count)
		# print(page_size)
	except Exception as e:
		print(e)
	finally:
		return result_list


db_dict = {
	'orgno' : '单位代码',
	'orgname' : '供电单位',
	'consno' : '户号',
	'consname' : '户名',
	'consphone' : '手机号',
	'consaddr' : '用电地址',
	'cbdno' : '抄表段编号',
	'cbdname' : '抄表段名称',
	'datadate' : '统计时间',
	'startdate' : '开始时间'
}	


'''
构造insert语句
INSERT INTO test_yxxt_phone_over5(orgno,orgname,consno,consname,consphone,consaddr,cbdno,cbdname,datadate)
 VALUES('374110004','南湖供电所','0181119078','王云远','13066091308','日照市东港区南湖镇南湖镇335省道大城子','RZN21231','大城子村东南4#配变','2018-03-15')
 ON DUPLICATE KEY UPDATE datadate = '2018-03-18';
'''
def get_sql_insert(data_list):
	sql_insert_list = []

	sql_insert = ''
	for item in data_list:
		sql_insert = 'INSERT INTO bus_yxxt_phone_over5('
		sql_insert2 = ' VALUES('
		
		for key in db_dict.keys():
			
			if key == 'startdate':
				sql_insert = sql_insert + key + ','
				sql_insert2 = sql_insert2 + '"' + item['datadate'] + '"' + ','
			else:
				sql_insert = sql_insert + key + ','
				sql_insert2 = sql_insert2 + '"' + item[key] + '"' + ','
		sql_insert = sql_insert[0:-1] + ')'
		sql_insert2 = sql_insert2[0:-1] + ')'
		sql_insert3 = ' ON DUPLICATE KEY UPDATE datadate ="' + item['datadate'] + '"'
		sql_insert = sql_insert + sql_insert2 + sql_insert3
		# print(sql_insert)
		sql_insert_list.append(sql_insert)

	return sql_insert_list


# def get_sql_insert(data_list):
# 	sql_insert_list = []

# 	sql_insert = ''
# 	for item in data_list:
# 		sql_insert = 'INSERT INTO bus_yxxt_phone_over5('
# 		sql_insert2 = ' SELECT '
		
# 		for key in db_dict.keys():
# 			sql_insert = sql_insert + key + ','
# 			sql_insert2 = sql_insert2 + '"' + item[key] + '"' + ','
# 		sql_insert = sql_insert[0:-1] + ')'
# 		sql_insert2 = sql_insert2[0:-1]
# 		sql_insert3 = ' FROM dual WHERE NOT EXISTS(SELECT id,consno FROM bus_yxxt_phone_over5 WHERE consno="'+ item['consno'] +'" AND datadate="'+ item['datadate'] +'")'
# 		sql_insert = sql_insert + sql_insert2 + sql_insert3
# 		sql_insert_list.append(sql_insert)

# 	return sql_insert_list


'''
爬虫
'''
def crawl():
	#登录，配置好cookie
	print('**********%s:正在登录**********' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
	login()
	print('**********%s:登录成功**********' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
	#查询第记录数
	print('**********%s:开始爬取数据**********' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
	pagestart = 1
	page_dict = query_page_count()
	page_size = '5000'
	# page_size = '50'

	page_count = 1
	record_count = page_dict['record_count']
	if (int(record_count) % int(page_size)) == 0:
		page_count = int(int(record_count) // int(page_size))
	else:
		page_count = int(int(record_count) // int(page_size)) + 1

	result_list = []
	print('**********%s:共%s条记录，每页%s条，共%s页**********' % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), record_count, page_size, page_count))
	for page in range(pagestart, int(page_count) + 1):
		print('**********%s:正在爬取第%s页，共%s页**********' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), page, page_count))
		result_list = query_1phone_for_5cons(pageno = str(page), pagesize = page_size)
		#存储数据
		print('**********%s:正在存储第%s页数据**********' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), page))
		sql_list = get_sql_insert(result_list)
		for sql in sql_list:
			dbhelper.db_insert(sql)
		print('**********%s:第%s页数据存储成功**********' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), page))

	print('**********%s:全部爬取完成**********' %(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
	

if __name__ == '__main__':
	#定义爬虫启动时间,每天1点0分
	h = 18
	m = 11
	while True:  
		now = datetime.datetime.now()  
		print(now.hour, now.minute)  
		if now.hour == h and now.minute == m:
			print('时间到了')
			crawl()
			continue
		# 每隔60秒检测一次  
		time.sleep(60)
	
	