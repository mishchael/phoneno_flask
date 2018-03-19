#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Date    : 2018-03-11 00:42:45
# @Author  : Michael (mishchael@gmail.com)

import dbhelper
import datetime
from flask import Flask
from flask import request
from flask import render_template
from flask import jsonify
import json

app = Flask(__name__)

@app.route('/')
def index():
	return 'Hello,This is index!'

@app.route('/api/area_sel')
def area_sel():
	return_data = []
	sel_all = {}
	sel_all['id'] = 1
	sel_all['text'] = '全部'
	sel_all['selected'] = 'true'
	return_data.append(sel_all)
	sql = 'SELECT DISTINCT consarea FROM bus_ydcj_power_consumption'
	db_data = dbhelper.db_select(sql)
	id_count = 2
	for row in db_data:
		row_dict = {}
		row_dict['id'] = id_count
		row_dict['text'] = row['consarea']
		id_count += 1
	
		return_data.append(row_dict)

	return_data = jsonify(return_data)
	# db_data = jsonify(db_data)
	return return_data

@app.route('/api/consname_sel', methods = ['POST'])
def consname_sel():
	consarea = request.form.get('area')
	return_data = []
	sql_area = 'SELECT DISTINCT consarea FROM bus_ydcj_power_consumption WHERE consarea = "' + consarea + '"'
	if consarea == '全部':
		sql_area = 'SELECT DISTINCT consarea FROM bus_ydcj_power_consumption'
	db_data_area = dbhelper.db_select(sql_area)
	for row in db_data_area:
		group_dict = {}
		group_dict['text'] = row['consarea']
		group_dict['children'] = []
		return_data.append(group_dict)

	sql_name = 'SELECT DISTINCT consnamefull,consarea FROM bus_ydcj_power_consumption WHERE consarea = "' + consarea + '"'
	if consarea == '全部':
		sql_name = 'SELECT DISTINCT consnamefull,consarea FROM bus_ydcj_power_consumption'
		

	db_data_name = dbhelper.db_select(sql_name)
	id_count = 1
	for row in db_data_name:
		row_dict = {}
		row_dict['id'] = id_count
		row_dict['text'] = row['consnamefull']
		for group in return_data:
			if group['text'] == row['consarea']:
				group['children'].append(row_dict)
		id_count += 1

	return_data = jsonify(return_data)
	return return_data
	
@app.route('/api/consno_sel', methods = ['POST'])
def consno_sel():
	consnamefull_list = json.loads(request.form.get('name'))
	return_data = []
	#根据用户名称查用户编号
	#构造分组
	sql_no = 'SELECT DISTINCT consno,consnamefull FROM bus_ydcj_power_consumption WHERE consnamefull IN ('
	for name in consnamefull_list:
		sql_no = sql_no + '"' + name + '",'
		group_dict = {}
		group_dict['text'] = name
		group_dict['children'] = []
		return_data.append(group_dict)

	sql_no = sql_no[:-1] + ')'
	db_data_no = dbhelper.db_select(sql_no)

	id_count = 1
	for row in db_data_no:
		row_dict = {}
		row_dict['id'] = id_count
		row_dict['text'] = row['consno']
		for group in return_data:
			if group['text'] == row['consnamefull']:
				group['children'].append(row_dict)
		id_count += 1

	return_data = jsonify(return_data)
	return return_data


@app.route('/get_data', methods = ['POST'])
def get_data():
	#获取post参数
	draw = int(request.form.get('draw'))
	#请求的起始行（比如：第一页： 0，第二页： 1*pagelength）
	start = int(request.form.get('start'))
	#每页的长度
	length = int(request.form.get('length'))
	#搜索
	search_value = request.form.get('search[value]')
	#排序的列，数字
	order_col = request.form.get('order[0][column]')
	#排序方向asc/desc
	order_dir = request.form.get('order[0][dir]')

	sql = 'SELECT consno,DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,consnamefull,consarea,pape,pape1,pape2,pape3,pape4,papr FROM bus_ydcj_power_consumption WHERE consno in ("0182653487","0182824005")'
	if search_value:
		sql = sql + r' AND consnamefull LIKE "%' + search_value + '%"'

	#排序
	column_dict = {}
	column_dict['0'] = 'consno'
	column_dict['1'] = 'datadate'
	column_dict['2'] = 'consnamefull'
	column_dict['3'] = 'consarea'
	column_dict['4'] = 'pape'
	column_dict['5'] = 'pape1'
	column_dict['6'] = 'pape2'
	column_dict['7'] = 'pape3'
	column_dict['8'] = 'pape4'
	column_dict['9'] = 'papr'
	if order_col and order_dir:
		col = column_dict[order_col]
		sql = sql + ' ORDER BY ' + col + ' ' + order_dir
	power_data = dbhelper.db_select(sql)
	
	#分页
	data_list = []
	recordsTotal = len(power_data)
	last_row_num = len(power_data)
	
	for i in range(start, start + length):
		if i < last_row_num:
			row = power_data[i]
			data_list.append(row)

	return_data = {}
	return_data['data'] = data_list
	return_data['draw'] = draw
	return_data['recordsTotal'] = recordsTotal
	return_data['recordsFiltered'] = recordsTotal

	return_data = jsonify(return_data)
	return return_data

@app.route('/api/query', methods = ['POST'])
def query():
	#获取post参数
	draw = int(request.form.get('draw'))
	#请求的起始行（比如：第一页： 0，第二页： 1*pagelength）
	start = int(request.form.get('start'))
	#每页的长度
	length = int(request.form.get('length'))
	#搜索
	search_value = request.form.get('search[value]')
	#排序的列，数字
	order_col = str(request.form.get('order[0][column]'))
	#排序方向asc/desc
	order_dir = request.form.get('order[0][dir]')
	#查询条件
	area = request.form.get('area')
	name_list = json.loads(request.form.get('name'))
	no_list = json.loads(request.form.get('no'))

	sql = ''
	#先按用户编号查询，如果用户编号为空，按用户名称查询，都为空，按区县查询
	if len(no_list) > 0:
		sql = 'SELECT consno,DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,consnamefull,consarea,pape,pape1,pape2,pape3,pape4,papr FROM bus_ydcj_power_consumption WHERE consno in ('
		for no in no_list:
			sql = sql + '"' + no + '",'
		sql = sql[:-1] + ')'
	elif len(name_list) > 0:
		sql = 'SELECT consno,DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,consnamefull,consarea,pape,pape1,pape2,pape3,pape4,papr FROM bus_ydcj_power_consumption WHERE consnamefull in ('
		for name in name_list:
			sql = sql + '"' + name + '",'
		sql = sql[:-1] + ')'
	elif area:
		area = area.replace('"', '')
		if area == '全部':
			sql = 'SELECT consno,DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,consnamefull,consarea,pape,pape1,pape2,pape3,pape4,papr FROM bus_ydcj_power_consumption'
		else:
			sql = 'SELECT consno,DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,consnamefull,consarea,pape,pape1,pape2,pape3,pape4,papr FROM bus_ydcj_power_consumption WHERE consarea ="' + area +'"'
	else:
		sql = 'SELECT consno,DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,consnamefull,consarea,pape,pape1,pape2,pape3,pape4,papr FROM bus_ydcj_power_consumption WHERE consno in ("0182653487","0182824005")'
	#搜索条件
	if search_value:
		sql = sql + r' AND consnamefull LIKE "%' + search_value + '%"'
	# return sql
	#排序
	column_dict = {}
	column_dict['0'] = 'consno'
	column_dict['1'] = 'datadate'
	column_dict['2'] = 'consnamefull'
	column_dict['3'] = 'consarea'
	column_dict['4'] = 'pape'
	column_dict['5'] = 'pape1'
	column_dict['6'] = 'pape2'
	column_dict['7'] = 'pape3'
	column_dict['8'] = 'pape4'
	column_dict['9'] = 'papr'
	if order_col and order_dir:
		col = column_dict[order_col]
		sql = sql + ' ORDER BY ' + col + ' ' + order_dir
	# return sql
	power_data = dbhelper.db_select(sql)
		
	#分页
	data_list = []
	recordsTotal = len(power_data)
	last_row_num = len(power_data)
	
	for i in range(start, start + length):
		if i < last_row_num:
			row = power_data[i]
			data_list.append(row)

	return_data = {}
	return_data['data'] = data_list
	return_data['draw'] = draw
	return_data['recordsTotal'] = recordsTotal
	return_data['recordsFiltered'] = recordsTotal

	return_data = jsonify(return_data)
	return return_data






@app.route('/datatable')
def datatable():
	return render_template('datatable.html')

@app.route('/chart_bar_data', methods = ['GET'])
def chart_bar_data():
	sql = 'SELECT consarea,SUM(pape) as pape,SUM(pape1) as pape1,SUM(pape2) as pape2,SUM(pape3) as pape3,SUM(pape4) as pape4 FROM bus_ydcj_power_consumption GROUP BY consarea'
	chart_data = dbhelper.db_select(sql)
	return_data = {}
	area_list = []
	pape_list = []
	pape1_list = []
	pape2_list = []
	pape3_list = []
	pape4_list = []
	for row in chart_data:
		area_list.append(row['consarea'])
		pape_list.append(round(row['pape'], 2))
		pape1_list.append(round(row['pape1'], 2))
		pape2_list.append(round(row['pape2'], 2))
		pape3_list.append(round(row['pape3'], 2))
		pape4_list.append(round(row['pape4'], 2))

	return_data['area'] = area_list
	return_data['pape'] = pape_list
	return_data['pape1'] = pape1_list
	return_data['pape2'] = pape2_list
	return_data['pape3'] = pape3_list
	return_data['pape4'] = pape4_list

	return_data = jsonify(return_data)
	return return_data

@app.route('/chart_cal_effectscatter_data')
def chart_cal_effectscatter_data():
	return_data = []
	sql = 'SELECT DATE_FORMAT(datadate,"%Y-%m-%d") as datadate,pape FROM bus_ydcj_power_consumption WHERE consno = "0180825628"'
	chart_data = dbhelper.db_select(sql)
	
	for row in chart_data:
		row_list =[]
		row_list.append(row['datadate'])
		row_list.append(row['pape'])
		return_data.append(row_list)

	return_data = jsonify(return_data)
	return return_data
		
@app.route('/chart_tree_data')
def chart_tree_data():
	return_data = {}
	
	sql = 'SELECT DISTINCT consno,consarea,consnamefull FROM bus_ydcj_power_consumption'
	chart_data = dbhelper.db_select(sql)
	area_name_dict = {}
	name_no_dict = {}
	return_data['name'] = '日照市机械铸造企业'
	return_data['children'] = []
	for row in chart_data:
		if row['consarea'] not in area_name_dict.keys():
			area_name_dict[row['consarea']] = []
		area_name_dict[row['consarea']].append(row['consnamefull'])
		if row['consnamefull'] not in name_no_dict.keys():
			name_no_dict[row['consnamefull']] = []
		name_no_dict[row['consnamefull']].append(row['consno'])
	# return_data_children = []
	for area,name in area_name_dict.items():
		child_area = {}
		child_area['name'] = area
		child_area['children'] = []
		for name_key,no in name_no_dict.items():
			if name_key in name:
				child_name = {}
				child_name['name'] = name_key
				child_name['value'] = ''
				for no_item in no:
					child_name['value'] = child_name['value'] + '.' + no_item
				child_name['value'] = child_name['value'][1:]
				child_area['children'].append(child_name)
		return_data['children'].append(child_area)
	

	return_data = jsonify(return_data)
	return return_data



@app.route('/chart')
def chart():
	return render_template('chart.html')








if __name__ == '__main__':
	app.debug= True
	app.run(host = '0.0.0.0')