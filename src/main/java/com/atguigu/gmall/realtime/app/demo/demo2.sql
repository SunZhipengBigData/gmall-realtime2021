create table dwd_page_common(
common MAP<STRING,STRING>,
page MAP<STRING,STRING>,
ts  BIGINT,
rowtime  TO_TIMESTAMP(FROM_UNIXTIME( ts / 1000, 'yyyy-MM-dd HH:mm:ss')) ,
watermark for rowtime as rowtime - interval '2' second
)
with
(
'connector' = 'kafka',
'topic' = 'dwd_display_log',
'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',
'properties.group.id' = 'demo2',
'format' = 'json',
'scan.startup.mode' = 'latest-offset'
)

---------------------
create table dwd_display_common(
display_type String,
page_id String,
item String,
item_type String,
sku_id String,
pos_id String ,
order String
)
with
(
'connector' = 'kafka',
'topic' = 'dwd_page_log',
'properties.bootstrap.servers' = 'hadoop101:9092,hadoop102:9092,hadoop103:9092',
'properties.group.id' = 'demo2',
'format' = 'json',
'scan.startup.mode' = 'latest-offset'
)
select
a.common['uid'] as uid,
count(case when b.common['mid'] is not null then 1 else 0 end) as midamount
from  dwd_page_common   a
left join
dwd_start_common b
on  a.common['uid']=b.common['uid']
and a.rowtime between b.rowtime  and b.rowtime +interval '10' second
group by
a.common['uid']


create   table  dwd_start_common(
common <String,String>,
start <String,String>,
ts BIGINT,
rowtime  TO_TIMESTAMP(FROM_UNIX(ts/1000,'yyyy-MM-dd HH:mm:ss' )),
watermark for  rowtime as rowtime- '2'  seconds
)

orderId  productName payType orderTime payTime
001      iphone   alipay  2018-12-26 04:53:22.0 2018-12-26 05:51:41.0
002      mac      card    2018-12-26 04:53:23.0 2018-12-26 05:53:22.0
004      cup      alipay  2018-12-26 04:53:38.0 2018-12-26 05:53:31.0



