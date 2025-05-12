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

# 创建函数 extract_username
```
CREATE OR REPLACE FUNCTION extract_username(input_string TEXT)
RETURNS TEXT AS $$
DECLARE
    start_pos INTEGER;
    end_pos INTEGER;
BEGIN
    -- 检查字符串是否以CN%3d开头
    IF input_string LIKE 'CN%3d%' THEN
        -- 计算名字的起始位置(跳过CN%3d)
        start_pos := 6;
        
        -- 查找第一个%2c的位置
        end_pos := position('%2c' IN input_string);
        
        -- 如果找到了%2c，提取中间的名字部分
        IF end_pos > 0 THEN
            RETURN substring(input_string FROM start_pos FOR end_pos - start_pos);
        ELSE
            -- 如果没有找到%2c，返回从CN%3d之后的所有内容
            RETURN substring(input_string FROM start_pos);
        END IF;
    ELSE
        -- 如果字符串不以CN%3d开头，返回NULL
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
```