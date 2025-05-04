# 创建user_traffic_view
```
-- 第一步：创建临时表 t1
CREATE VIEW user_traffic_view AS 
WITH t1 AS (
    SELECT 
        (regexp_matches(acct, 'CN%3d([^%]+)'))[1] AS tmp_username,
        SUM(uptraffic::numeric) AS uptraffic,
        SUM(downtraffic::numeric) AS downtraffic
    FROM 
        acctreport
    GROUP BY 
        (regexp_matches(acct, 'CN%3d([^%]+)'))[1]
),
-- 第二步：创建最终的新表
 t2 AS (
    SELECT 
    gu.username AS username,
    t1.uptraffic,
    t1.downtraffic,
    gu.gwid,
    gu."group",
    gu.online,
    gu.datelimit,
    gu.delete_mark,
    gu.id as userid,
    substring(gu.remark, 'ISP-1-(.*)') AS remark
FROM 
    t1
RIGHT JOIN 
    gw_users gu 
ON 
    t1.tmp_username = gu.username
),
 t3 AS (
		SELECT 
    t2.username AS username,
    t2.uptraffic,
    t2.downtraffic,
    t2.gwid,
    t2."group",
    t2.online,
    t2.datelimit,
    t2.delete_mark,
    t2.userid,
    s.remark as remark,
    s.id as sid
FROM 
    t2
LEFT JOIN 
    gw_bandwidth_strategy as s 
ON 
    t2.remark = s.id AND t2.gwid = s.gwid
 )

SELECT 
    g.name as gateway_name,
    t3.userid,
    t3.username,
    t3.uptraffic,
    t3.downtraffic,
    t3."group",
    t3.online,
    t3.datelimit,
    t3.delete_mark,
    t3.remark,
    t3.sid
FROM t3
JOIN gateway g
ON g.id::varchar = t3.gwid;
```