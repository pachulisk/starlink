-- 创建user_auth_gateways_view
CREATE OR REPLACE VIEW user_auth_gateways_view AS
WITH t0 AS (
        SELECT
    uag.created_at as created_at,
    uag.userid as userid,
    uag.gwid as gwid,
    g.name as gateway_name
FROM
    user_auth_gateways uag
LEFT JOIN
    gateway as g
ON
    uag.gwid = g.id::VARCHAR
),
t1 AS (
  -- 第一步：将字符串格式的流量值转换为数字
  SELECT 
    t0.created_at as created_at,
    t0.userid as userid,
    t0.gwid as gwid,
    t0.gateway_name as gateway_name,
    ua.username as username
  FROM t0
  LEFT JOIN 
    user_auth as ua
  ON
    t0.userid = ua.global_id
)

SELECT 
  created_at,
  gateway_name, 
  gwid,
  userid,
  username
FROM t1;

-- 创建user_strategy_logs_view
CREATE OR REPLACE VIEW user_strategy_logs_view AS
WITH t0 AS (
        SELECT
    usl.created_at as created_at,
    usl.user_id as userid,
    usl.gwid as gwid,
    usl.new_sid as sid,
    s.remark as remark
FROM
    user_strategy_logs usl
LEFT JOIN
    gw_bandwidth_strategy as s 
ON
    usl.new_sid = s.id AND usl.gwid = s.gwid
),
converted_data AS (
  -- 第一步：将字符串格式的流量值转换为数字
  SELECT 
    t0.created_at as record_time,
    t0.remark as remark,
    t0.sid as sid,
    t0.gwid as gwid,
    t0.userid as userid,
    gu.username as username,
    -- 提取数字部分并转换为数值类型（处理"G"和"GB"后缀）
    CAST(REGEXP_REPLACE(t0.remark, '[^0-9.]', '', 'g') AS NUMERIC) AS total_traffic
  FROM t0
  LEFT JOIN 
    gw_users as gu
  ON
    t0.userid = gu.id AND t0.gwid = gu.gwid
),
increment_calculation AS (
  -- 第二步：计算增量（包含月初清零逻辑）
  SELECT 
    converted_data.record_time as record_time,
    converted_data.remark as remark,
    converted_data.sid as sid,
    converted_data.gwid as gwid,
    converted_data.userid as userid,
    converted_data.username as username,
    record_time AS increment_time,
    CASE 
      -- 第一条记录：增量等于当前流量值
      WHEN LAG(record_time) OVER (ORDER BY record_time) IS NULL THEN total_traffic
      -- 跨月记录（月初）：从0开始，增量等于当前流量值
      WHEN DATE_TRUNC('month', record_time) <> DATE_TRUNC('month', LAG(record_time) OVER (ORDER BY record_time)) THEN total_traffic
      -- 同月内：正常计算差值
      ELSE total_traffic - LAG(total_traffic) OVER (ORDER BY record_time)
    END AS traffic_increment
  FROM converted_data
  ORDER BY record_time
)
SELECT 
  record_time,
  remark, 
  sid,
  gwid,
  userid,
  increment_time,
  traffic_increment,
  username
FROM increment_calculation;


-- 创建user_monthly_strategy_view
CREATE OR REPLACE VIEW user_monthly_strategy_view AS 
WITH t0 AS (
		SELECT 
    ums.gwid AS gwid,
    ums.userid as userid,
    ums.sid as sid,
    s.remark as remark,
    ums.created_at as created_at
FROM 
    user_monthly_strategy ums
LEFT JOIN 
    gw_bandwidth_strategy as s 
ON 
    ums.sid = s.id AND ums.gwid = s.gwid
 ),
t AS (
SELECT 
    t0.gwid as gwid,
    t0.userid as userid,
    t0.sid as sid,
    t0.remark as remark,
    t0.created_at as created_at,
    utv.username as username,
    utv.gateway_name as gateway_name
FROM
    t0
LEFT JOIN
    user_traffic_view as utv
ON
    t0.gwid = utv.gwid AND t0.userid = utv.userid
)
SELECT 
    t.gwid,
    t.userid,
    t.sid,
    t.remark,
    t.created_at,
    t.username,
    t.gateway_name
FROM 
    t;

# 创建get_current_year_month函数
CREATE OR REPLACE FUNCTION get_current_year_month()
RETURNS TEXT AS $$
BEGIN
    -- 使用to_char函数将当前日期转换为'yyyy-mm'格式
    RETURN to_char(CURRENT_DATE, 'yyyy-mm');
END;
$$ LANGUAGE plpgsql;

# SELECT get_current_year_month() as yyyymm;

-- DROP VIEW acctreport_monthly_view;

-- 创建acctreport_monthly_view
CREATE OR REPLACE VIEW acctreport_monthly_view AS

SELECT uptraffic, downtraffic, acct
FROM acctreport
WHERE SUBSTRING(CAST(happendate AS VARCHAR) FROM 1 FOR 7) = get_current_year_month();

# 删除acctreport_view
DROP VIEW acctreport_view;

# 创建acctreport_view
```
CREATE OR REPLACE VIEW acctreport_view AS 

SELECT 
    extract_username(acct) AS acct,
    uptraffic::numeric AS uptraffic,
    downtraffic::numeric AS downtraffic,
    gwid,
    happendate
FROM 
    acctreport
```
-- 创建user_traffic_monthly_view

CREATE OR REPLACE VIEW user_traffic_monthly_view AS 
-- 第零步: 创建临时表t0, yyyymm是get_current_year_month
WITH t0 AS (
    SELECT get_current_year_month() as yyyymm
),
-- 第一步：创建临时表 t1
 t1 AS (
    SELECT 
        extract_username(acct) AS tmp_username,
        SUM(uptraffic::numeric) AS uptraffic,
        SUM(downtraffic::numeric) AS downtraffic
    FROM 
        acctreport a
    JOIN 
        t0
    ON
        SUBSTRING(CAST(a.happendate AS VARCHAR) FROM 1 FOR 7) = t0.yyyymm
    GROUP BY 
        extract_username(acct)
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
    t3.gwid,
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

# 删除user_traffic_view
DROP VIEW user_traffic_view;
# 创建user_traffic_view
```
-- 第一步：创建临时表 t1
CREATE OR REPLACE VIEW user_traffic_view AS 
WITH t1 AS (
    SELECT 
        extract_username(acct) AS tmp_username,
        SUM(uptraffic::numeric) AS uptraffic,
        SUM(downtraffic::numeric) AS downtraffic
    FROM 
        acctreport
    GROUP BY 
        extract_username(acct)
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
    t3.gwid,
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

# 创建函数 extract_username
```
CREATE OR REPLACE FUNCTION extract_username(input_string TEXT)
RETURNS TEXT AS $$
DECLARE
    start_pos INTEGER;
    end_pos INTEGER;
    username text;
BEGIN
    -- 检查字符串是否以CN%3d开头
    IF input_string LIKE 'CN%3d%' THEN
        -- 计算名字的起始位置(跳过CN%3d)
        start_pos := 6;
        
        -- 查找第一个%2c的位置
        end_pos := position('%2c' IN input_string);
        
        -- 如果找到了%2c，提取中间的名字部分
        IF end_pos > 0 THEN
            username = substring(input_string FROM start_pos FOR end_pos - start_pos);
        ELSE
            -- 如果没有找到%2c，返回从CN%3d之后的所有内容
            username = substring(input_string FROM start_pos);
        END IF;
        -- 替换%2d为'-'
        username := replace(username, '%2d', '-');
        username := replace(username, '%2f', '/');
        RETURN username;
    ELSE
        -- 如果字符串不以CN%3d开头，返回NULL
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
```

# 创建gw_user_traffic_view
```
CREATE OR REPLACE VIEW gw_user_traffic_view AS
SELECT
    happendate,
    gwid,
    SUM(uptraffic) AS up,
    SUM(downtraffic) AS down
FROM
    acctreport_view
GROUP BY
    happendate, gwid
ORDER BY
    happendate, gwid;
    
```

# 创建gw_users_count
CREATE OR REPLACE VIEW gw_users_count AS
SELECT COUNT(*) AS count
FROM user_traffic_view;    

-- 创建gateway_monthly_view

CREATE OR REPLACE VIEW gateway_monthly_view AS 

WITH t3 AS (
    SELECT 
        g.id,
        tr.up,
        tr.down,
        u.count as user
    FROM
        gateway g
    JOIN
        total_traffic_monthly_by_gwid tr
    ON
        g.id::varchar = tr.gwid
    JOIN
        gw_user_count_group_by_gwid u
    ON 
        g.id::varchar = u.gwid
  GROUP BY 
    g.id,
    tr.up,
    tr.down,
    u.count
)

SELECT 
    g.id,
    g.name,
    g.username,
    g.port,
    g.address,
    g.password,
    g.serial_no,
    g.client_name,
    g.enable_time,
    g.online,
    g.fleet,
    t3.up,
    t3.down,
    t3.user AS user_count
FROM gateway g
LEFT JOIN t3
ON g.id::varchar = t3.id::varchar;


# 创建gateway_view

CREATE OR REPLACE VIEW gateway_view AS 

WITH t3 AS (
    SELECT 
        g.id,
        tr.up,
        tr.down,
        u.count as user
    FROM
        gateway g
    JOIN
        total_traffic_group_by_gwid tr
    ON
        g.id::varchar = tr.gwid
    JOIN
        gw_user_count_group_by_gwid u
    ON 
        g.id::varchar = u.gwid
  GROUP BY 
    g.id,
    tr.up,
    tr.down,
    u.count
)

SELECT 
    g.id,
    g.name,
    g.username,
    g.port,
    g.address,
    g.password,
    g.serial_no,
    g.client_name,
    g.enable_time,
    g.online,
    g.fleet,
    t3.up,
    t3.down,
    t3.user AS user_count
FROM gateway g
LEFT JOIN t3
ON g.id::varchar = t3.id::varchar;

# 创建temp_acct_traffic_view
CREATE OR REPLACE VIEW temp_acct_traffic_view AS 
    SELECT 
        extract_username(acct) AS tmp_username,
        SUM(uptraffic::numeric) AS uptraffic,
        SUM(downtraffic::numeric) AS downtraffic
    FROM 
        acctreport
    GROUP BY 
        extract_username(acct);

# 创建total_traffic_by_gwid
CREATE OR REPLACE VIEW total_traffic_group_by_gwid AS
SELECT gwid, SUM(uptraffic::numeric) as up, SUM(downtraffic::numeric) as down from "user_traffic_view"
GROUP BY gwid;

-- 创建total_traffic_monthly_by_gwid
CREATE OR REPLACE VIEW total_traffic_monthly_by_gwid AS
SELECT gwid, SUM(uptraffic::numeric) as up, SUM(downtraffic::numeric) as down from "user_traffic_monthly_view"
GROUP BY gwid;

# 创建total_traffic
CREATE OR REPLACE VIEW total_traffic AS
SELECT SUM(uptraffic::numeric) as up, SUM(downtraffic::numeric) as down from "user_traffic_view";

# 创建gateway_count
CREATE OR REPLACE VIEW gateway_count AS 
SELECT COUNT(*) AS count FROM gateway;

CREATE OR REPLACE VIEW temp_gateway_traffic_view AS 
    SELECT 
        g.id,
        tr.up,
        tr.down,
        u.count as user
    FROM
        gateway g
    JOIN
        total_traffic_group_by_gwid tr
    ON
        g.id::varchar = tr.gwid
    JOIN
        gw_user_count_group_by_gwid u
    ON 
        g.id::varchar = u.gwid
  GROUP BY 
    g.id,
    tr.up,
    tr.down,
    u.count;