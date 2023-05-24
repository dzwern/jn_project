# -*-conding:utf-8 -*-
# !/usr/bin/env python3
"""
# @Time    : 2023/3/31 15:31
# @Author  : diaozhiwei
# @description: 数据库测试
# @update:
"""
from jn_modules.dingtalk.DingTalk import DingTalk
from jn_modules.mysql.jnmtMySQL import jnMysql
from jn_modules.func import utils


# 执行SQL
@utils.print_execute_time
def get_gj_orders():
    sql = """
select
	now() as now_time,
	dept_name,
	member_id,
	concat('【', GROUP_CONCAT(concat_ws('|', product_name, trim(both '"' from JSON_EXTRACT(specification_values, '$[0].value')), quantity) separator '】【'), '】') as 'mess_pro',
	o_state as 'order_state',
	nick_name ,
	wecaht_number,
	user_name,
	create_time ,
	order_sn,
	yx_type,
	receiver_province,
	receiver_city,
	order_amount,
	refund_amount,
	order_amount-refund_amount as real_amount,
	performance_type,
	memo
	from
		(
		select
			o.id as order_id,d.dept_name,o.member_id,oi.product_name,oi.specification_values,oi.quantity,o.order_state,o.sys_user_id,o.wechat_id,o.refund_amount,
			m.add_wechat_time,m.incoming_line_time,m.source_type,m.fission_type,m.clue_state,m.member_source,m.member_source_level2 ,o.trade_time,o.create_time,o.order_sn,o.order_type,o.order_amount,o.complate_date,u.nick_name,w.wecaht_number,dd2.dict_label as yx_type,dd1.dict_label as o_state,dd3.dict_label as performance_type,m.member_type,o.receiver_province,o.receiver_city,ori.no_performance_type,u.user_name,o.memo
		from
		(
			select * 
			from t_orders as o 
			where o.tenant_id in(21)
						and EXISTS(select 1 from t_member where id = o.member_id and tenant_id in(21))					
		) as o
		left join sys_user as u on
			u.user_id = o.sys_user_id
			and o.tenant_id = 21
		left join sys_dept as d on
			d.dept_id = u.dept_id
			and o.tenant_id = 21
		left join t_order_item as oi on
			oi.order_id = o.id
			and o.tenant_id = 21
		left join t_member as m on
			m.id = o.member_id
			and o.tenant_id = 21
		left join t_order_rel_info as ori on
			ori.orders_id = o.id
			and o.tenant_id = 21
		left join t_wechat as w on
			w.id = o.wechat_id
			and o.tenant_id = 21
		left join sys_dict_data as dd2 on
			dd2.dict_type = 'order_type'
			and dd2.dict_value = o.order_type
			and dd2.tenant_id = 21 
		left join sys_dict_data as dd1 on dd1.dict_type = 'order_state'
			and dd1.dict_value = o.order_state
			AND dd1.tenant_id = 21	
		left join sys_dict_data as dd3 on dd3.dict_type = 'marketing_type'
			and dd3.dict_value = ori.no_performance_type
			AND dd3.tenant_id = 21
			where 1 = 1
			and o.tenant_id = 21
			and not exists 
			(
				select
					1
				from
					t_order_hang_up
				where
					order_id = o.id
			)--  ！剔除挂起单子！
	) as t
	group by
		order_id

"""
    df1 = hhx_sql1.get_DataFrame_PD(sql)
    return df1


def get_gj_user():
    sql = """
select
			now() as now_time ,
			m.id as 'id',
			m.amount as 'amount',	
			m.amount_ecommerce as 'amount_ecommerce',		
			d.dept_name as 'dept_name',
			m.create_time as 'create_time',
			m.incoming_line_time as 'incoming_line_time',
			m.add_wechat_time as 'add_wechat_time',	
			w.wecaht_number as 'wecaht_number',
			u.user_name as 'user_name',
			u.nick_name as 'nick_name',	 
			dict_member_source.dict_label as 'member_source',
      dict_member_source2.dict_label as 'member_source_level2',
			dict_member_identity.dict_label as 'member_identity',
			case when m.is_check_form = 1 then '是' when m.is_check_form = 0 then '否'  else '未设置' end as 'check_form'
		from
			t_member as m
		left join sys_user as u on
			u.user_id = m.sys_user_id and m.tenant_id =21
		left join sys_dept as d on
			d.dept_id = u.dept_id and m.tenant_id =21
		left join t_wechat as w on
			w.id = m.wechat_id and m.tenant_id =21
		left join sys_dict_data as dict_member_source on  dict_member_source.dict_value = m.member_source and dict_member_source.tenant_id = 21 and dict_member_source.dict_type = 'member_source'
		left join sys_dict_data as dict_member_source2 on  dict_member_source2.dict_value = m.member_source_level2 and dict_member_source2.tenant_id = 21 and dict_member_source2.dict_type  in ('member_source_applets_login','member_source_online_retailer_order','member_source_touliu_add_wechat','member_source_touliu_clue','member_source_form','member_source_offline_huoke','member_source_platform_acquisition','member_source_online_retailer_zb')
		left join sys_dict_data as dict_member_identity on  dict_member_identity.dict_value = m.member_identity and dict_member_identity.tenant_id = 21 and dict_member_identity.dict_type = 'member_identity'
		where
			1 = 1
			and m.tenant_id =21

"""
    df2 = hhx_sql1.get_DataFrame_PD(sql)
    return df2


def get_gj_pro():
    sql = """
  select
	now() as 'now_time'
	,sku_id
	,dept_name
	,member_id
	,order_sn
	,product_name
	,TRIM(BOTH '"' from JSON_EXTRACT(specification_values, '$[0].value')) as 'unit'
	,quantity
	,sku_price
	,real_price
	,CONCAT('【', GROUP_CONCAT(CONCAT_WS('|', product_name, TRIM(BOTH '"' from JSON_EXTRACT(specification_values, '$[0].value')), quantity) separator '】【'), '】') as 'mess_pro' 
	,o_state as 'order_state'
	,nick_name
	,wecaht_number  
	,create_time
	,receiver_province
	,receiver_city
	,order_amount-refund_amount AS 'real_amount'
	from
	(
		select o.id as order_id, d.dept_name, o.member_id, oi.id as item_id, oi.product_name, oi.specification_values, oi.quantity, o.order_state, o.sys_user_id, o.wechat_id, m.add_wechat_time, m.incoming_line_time, o.trade_time, o.create_time, o.order_sn, o.order_type, o.order_amount, r.member_label_name,o.complate_date,o.refund_amount,
		u.nick_name, w.wecaht_number, dd2.dict_label as yx_type, dd1.dict_label as o_state ,m.member_type, o.receiver_province, o.receiver_city, ori.no_performance_type,u.user_name, sku.sn as sku_sn, oi.id as sku_id,oi.sku_price, oi.real_price,u.user_id
		from
		(
			select * 
			from t_orders as o 
			where o.tenant_id in(21)
						and EXISTS(select 1 from t_member where id = o.member_id and tenant_id in(21))					
		) as o
		LEFT JOIN sys_user as u on u.user_id = o.sys_user_id
		AND u.tenant_id = 21 
		left JOIN t_member_label_relation as r on  r.member_id  = o.member_id 
		AND r.tenant_id = 21 
		left JOIN sys_dept as d on d.dept_id = u.dept_id
		AND d.tenant_id = 21 
		left join t_order_item as oi on oi.order_id = o.id
		AND oi.tenant_id = 21 
		left JOIN t_sku as sku on sku.id = oi.sku_id
		AND sku.tenant_id = 21 
		LEFT JOIN t_member as m on m.id = o.member_id
		AND m.tenant_id = 21 
		INNER JOIN t_order_rel_info as ori on ori.orders_id = o.id
		AND ori.tenant_id = 21
		LEFT JOIN t_wechat as w on w.id = o.wechat_id
		AND w.tenant_id = 21
		left join sys_dict_data as dd2 on dd2.dict_type = 'order_type'
			and dd2.dict_value = o.order_type
			AND dd2.tenant_id = 21
		left join sys_dict_data as dd1 on dd1.dict_type = 'order_state'
			and dd1.dict_value = o.order_state
			AND dd1.tenant_id = 21	
		where 1 = 1
			 AND o.tenant_id = 21 
			and not exists (
			select
				1
			from
				t_order_hang_up
			where
				order_id = o.id)
	) as t
	GROUP BY order_id, item_id
"""
    df3 = hhx_sql1.get_DataFrame_PD(sql)
    return df3


def get_gj_retailer_order():
    sql = """

WITH gj_retailer_order AS
(
	SELECT 
		 now() as now_time,
		 orpl.tenant_id,
		 member_id,
		 orpl.create_time,
		 orp.`name` as 'plan_name',
		 orp.remark,
		 CASE
		  WHEN orp.re_allocation = 0 THEN '否'
			WHEN orp.re_allocation = 1 THEN '是'
		 END AS 're_allocation', -- 是否重新分配:0(否)，1(是)
		 sdd.dict_label as 'member_source',
		 CASE 
			WHEN get_type =1 THEN '自动获取' 
			WHEN get_type =0 THEN '手动上传' 
		 END AS 'get_type',    
		 CASE 
			WHEN customer_exists =1 THEN '存在' 
			WHEN customer_exists =0 THEN '不存在' 
		 END AS 'customer_exists',        
		 REPLACE(product,'\n','') AS 'mess_pro',
		 order_create_time,
		 order_amount,
		 replace(order_no,'\n','') AS 'order_no'
	FROM
			t_online_retailer_plan_log as orpl
		left join t_online_retailer_plan as orp on orp.id = orpl.online_retailer_plan_id  and orp.tenant_id = 8
		left join sys_user as u on u.user_id = orp.sys_user_id  and orpl.tenant_id = 8 
		left join sys_dept as sd on sd.dept_id = u.dept_id and sd.tenant_id = 8  		
		left join sys_dict_data as sdd on  sdd.dict_value = orp.source and sdd.tenant_id = 8 and sdd.dict_type IN ('online_retailer_source_ds','clue_source_channel','online_retailer_source_zb')
		where 1 = 1  and orpl.tenant_id = 8 AND orp.remark like '国酱'

	UNION ALL

	SELECT 
		 now() as now_time,  
		 orpl.tenant_id,
		 member_id,
		 orpl.create_time,
		 orp.`name` as 'plan_name',
		 orp.remark,
 		 CASE
		  WHEN orp.re_allocation = 0 THEN '否'
			WHEN orp.re_allocation = 1 THEN '是'
		 END AS 're_allocation', -- 是否重新分配:0(否)，1(是)
		 sdd.dict_label as 'member_source',
		 CASE 
			WHEN get_type =1 THEN '自动获取' 
			WHEN get_type =0 THEN '手动上传' 
		 END AS 'get_type',    
		 CASE 
			WHEN customer_exists =1 THEN '存在' 
			WHEN customer_exists =0 THEN '不存在' 
		 END AS 'customer_exists',        
		 REPLACE(product,'\n','') AS 'mess_pro',
		 order_create_time,	 
		 order_amount,
		 replace(order_no,'\n','') AS 'order_no'
	FROM
			t_online_retailer_plan_log as orpl
		left join t_online_retailer_plan as orp on orp.id = orpl.online_retailer_plan_id  and orp.tenant_id = 21
		left join sys_user as u on u.user_id = orp.sys_user_id  and orpl.tenant_id = 21 
		left join sys_dept as sd on sd.dept_id = u.dept_id and sd.tenant_id = 21  		
		left join sys_dict_data as sdd on  sdd.dict_value = orp.source and sdd.tenant_id = 21 and sdd.dict_type IN ('online_retailer_source_ds','clue_source_channel','online_retailer_source_zb')
		where 1 = 1  and orpl.tenant_id = 21 
)
select * from gj_retailer_order;
"""
    df4 = hhx_sql1.get_DataFrame_PD(sql)
    return df4


# 保存SQL
def save_gjo(df1):
    sql = '''
    INSERT INTO gj_order
         (
         now_time,dept_name,member_id,mess_pro,order_state,
         nick_name,wecaht_number,user_name,create_time,order_sn,
         yx_type,receiver_province,receiver_city,order_amount,refund_amount,
         real_amount,performance_type,memo)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
         now_time= VALUES(now_time)
         ,dept_name= VALUES(dept_name)
         ,member_id= VALUES(member_id)
         ,mess_pro= VALUES(mess_pro)
         ,order_state= VALUES(order_state)
         ,nick_name= VALUES(nick_name)
         ,wecaht_number= VALUES(wecaht_number)
         ,user_name= VALUES(user_name)
         ,create_time= VALUES(create_time)
         ,order_sn= VALUES(order_sn)
         ,yx_type= VALUES(yx_type)
         ,receiver_province= VALUES(receiver_province)
         ,receiver_city= VALUES(receiver_city)
         ,order_amount= VALUES(order_amount)
         ,refund_amount= VALUES(refund_amount)
         ,real_amount= VALUES(real_amount)
         ,performance_type= VALUES(performance_type)
         ,memo= VALUES(memo)
    '''
    hhx_sql2.executeSqlManyByConn(sql, df1.values.tolist())


def save_gju(df2):
    sql = '''
INSERT INTO gj_user
     (now_time,id,amount,amount_ecommerce,dept_name,create_time,incoming_line_time,add_wechat_time
     ,wecaht_number,user_name,nick_name,member_source,member_source_level2,member_identity,check_form)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
     now_time= VALUES(now_time)
     ,id= VALUES(id)
     ,amount= VALUES(amount)
     ,amount_ecommerce= VALUES(amount_ecommerce)
     ,dept_name= VALUES(dept_name)
     ,create_time= VALUES(create_time)
     ,incoming_line_time= VALUES(incoming_line_time)
     ,add_wechat_time= VALUES(add_wechat_time)
     ,wecaht_number= VALUES(wecaht_number)
     ,user_name= VALUES(user_name)
     ,nick_name= VALUES(nick_name)
     ,member_source= VALUES(member_source)
     ,member_source_level2= VALUES(member_source_level2)
     ,member_identity= VALUES(member_identity)
     ,check_form= VALUES(check_form)
'''
    hhx_sql2.executeSqlManyByConn(sql, df2.values.tolist())


def save_gjp(df3):
    sql = '''
INSERT INTO gj_pro
     (now_time,sku_id,dept_name,member_id,order_sn,product_name,unit,quantity
     ,sku_price,real_price,mess_pro,order_state,nick_name,wecaht_number,create_time,receiver_province,receiver_city,real_amount)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
     now_time= VALUES(now_time)
     ,sku_id= VALUES(sku_id)
     ,dept_name= VALUES(dept_name)
     ,member_id= VALUES(member_id)
     ,order_sn= VALUES(order_sn)
     ,product_name= VALUES(product_name)
     ,unit= VALUES(unit)
     ,quantity= VALUES(quantity)
     ,sku_price= VALUES(sku_price)
     ,real_price= VALUES(real_price)
     ,mess_pro= VALUES(mess_pro)
     ,order_state= VALUES(order_state)
     ,nick_name= VALUES(nick_name)
     ,wecaht_number= VALUES(wecaht_number)
     ,create_time= VALUES(create_time)
     ,receiver_province= VALUES(receiver_province)
     ,receiver_city= VALUES(receiver_city)
     ,real_amount= VALUES(real_amount)
'''
    hhx_sql2.executeSqlManyByConn(sql, df3.values.tolist())


def save_gjr(df4):
    sql = '''
INSERT INTO gj_retailer_order
     (now_time,tenant_id,member_id,create_time,plan_name,remark,re_allocation,member_source,get_type,customer_exists
     ,mess_pro,order_create_time,order_amount,order_no)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
     now_time= VALUES(now_time)
     ,tenant_id= VALUES(tenant_id)
     ,member_id= VALUES(member_id)
     ,create_time= VALUES(create_time)
     ,plan_name= VALUES(plan_name)
     ,remark= VALUES(remark)
     ,re_allocation= VALUES(re_allocation)
     ,member_source= VALUES(member_source)
     ,get_type= VALUES(get_type)
     ,customer_exists= VALUES(customer_exists)
     ,mess_pro= VALUES(mess_pro)
     ,order_create_time= VALUES(order_create_time)
     ,order_amount= VALUES(order_amount)
     ,order_no= VALUES(order_no)  
'''
    hhx_sql2.executeSqlManyByConn(sql, df4.values.tolist())


def main():
    df_orders = get_gj_orders()
    df_orders=df_orders
    print(df_orders)
    save_gjo(df_orders)
    # 报警发送钉钉群


if __name__ == '__main__':
    # 钉钉open
    hhx_sql1 = jnMysql('crm_tm_jnmt', 'xiehao', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    hhx_sql2 = jnMysql('fyjf_dx', 'xiehao', 'dsf#4oHGd', 'rm-2ze4184a0p7wd257yko.mysql.rds.aliyuncs.com')
    main()








