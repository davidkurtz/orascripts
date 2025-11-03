REM gfcash.sql
alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss';
WITH FUNCTION tsround(p_in IN TIMESTAMP, p_len INTEGER) RETURN timestamp IS
l_date VARCHAR2(20);
l_secs NUMBER;
l_date_fmt VARCHAR2(20) := 'J';
l_secs_fmt VARCHAR2(20) := 'SSSSS.FF9';
BEGIN
l_date := TO_CHAR(p_in,l_date_fmt);
--l_secs := ROUND(TO_NUMBER(TO_CHAR(p_in,l_secs_fmt)),p_len);
l_secs := FLOOR(TO_NUMBER(TO_CHAR(p_in,l_secs_fmt))/p_len)*p_len;
IF l_secs >= 86400 THEN
  l_secs := l_secs - 86400;
  l_date := l_date + 1;
END IF;
RETURN TO_TIMESTAMP(l_date||l_secs,l_date_fmt||l_secs_fmt);
END;
t as (
select TRUNC(SYSDATE-8) begindttm
,      SYSDATE enddttm
,      120 num_secs
from dual
), m0 as (
select TRUNC(CAST(min(begindttm) AS DATE),'hh24') min_sample_time
,      CAST(max(enddttm) AS DATE) max_sample_time
,      num_secs
from   t
), m1 as (
select min_sample_time
,      ceil((max_sample_time-min_sample_time)*1440) num_samples
,      num_secs
from   m0
), m as (
select /*+MATERIALIZE*/ min_sample_time+((level-1)*num_secs/86400) sample_time, level
from   m1
connect by level <= m1.num_samples
), c as (
select /*+MATERIALIZE LEADING(T)*/ DISTINCT c.consumer_group_id, c.consumer_group_name
from   t
,      dba_hist_snapshot x
,      DBA_HIST_RSRC_CONSUMER_GROUP c
WHERE  x.dbid = c.dbid
and    x.instance_number = c.instance_number
and    x.snap_id = c.snap_id
And    x.end_interval_time > t.begindttm
AND    x.begin_interval_time < t.enddttm
), h AS (
SELECT /*+LEADING(t x h c) USE_NL(H)*/ 
       h.instance_number, h.sample_id, h.usecs_per_row, t.num_secs
,      CAST(tsround(h.sample_time,t.num_secs) AS DATE) sample_time
,      CASE WHEN h.module IS NULL       THEN REGEXP_SUBSTR(h.program, '[^@]+',1,1)
            --WHEN h.module LIKE 'PSAE.%' THEN REGEXP_SUBSTR(h.module, '[^.]+',1,2) 
            ELSE                             REGEXP_SUBSTR(h.module, '[^.@]+',1,1) 
       END AS module2
,      h.module, h.event, h.user_id, h.client_id
,      CASE WHEN h.module LIKE 'DBMS_SCHEDULER%' THEN REGEXP_REPLACE(h.action, '[0-9]{4,}','<n>') 
            WHEN regexp_substr(h.program,'[^@.]+') IN('rman') THEN null
            WHEN h.module = 'REST' THEN REGEXP_REPLACE(h.action,'[({]?[a-fA-F0-9]{8}[-]?([a-fA-F0-9]{4}[-]?){3}[a-fA-F0-9]{12}[})]?','<GUID>')
            ELSE h.action END action
,      regexp_substr(h.program,'[^@.]+') program1
,      substr(regexp_substr(h.program,'\([[:alpha:]]+'),2) program2
--,      CASE WHEN UPPER(program) LIKE 'PSNVS%' THEN substr(regexp_substr(h.action,':([A-Z0-9_-])+',1,1,'i'),2) END report_id
--,      CASE WHEN UPPER(program) LIKE 'PSNVS%' THEN substr(regexp_substr(h.action,':([A-Z0-9_-])+',1,2,'i'),2) END business_unit
,      c.consumer_group_name, h.consumer_group_id
,      h.service_hash, s.name service_name
,      h.in_hard_parse, h.in_parse, h.in_sql_execution, h.in_plsql_execution, h.in_inmemory_query
FROM   t
,      dba_hist_snapshot x
,      dba_hist_Active_Sess_history h
         left outer join c
           ON   c.consumer_group_id = h.consumer_group_id
         left outer join v$services s
           ON s.name_hash = h.service_hash
WHERE  x.dbid = h.dbid
and    x.instance_number = h.instance_number
and    x.snap_id = h.snap_id
And    x.end_interval_time > t.begindttm
AND    x.begin_interval_time < t.enddttm
AND    h.sample_time BETWEEN t.begindttm AND t.enddttm
--and h.sql_id = '3v6v0tgyagmjb'
), x as (
SELECT /*+LEADING(M H) USE_HASH(R)*/ h.sample_time, h.usecs_per_row, h.num_secs, h.instance_number, h.sample_id
,      NVL(h.event,'CPU+CPU Wait') event
,      h.module, h.module2
,      NVL(h.action,'<NULL>') action
,      h.program1, h.program2
,      NVL(u.username,h.user_id) username
,      NVL(NVL(h.consumer_Group_name,h.consumer_group_id),'<NULL>') consumer_Group_name, h.service_name
,      h.in_hard_parse, h.in_parse, h.in_sql_execution, h.in_plsql_execution, h.in_inmemory_query
FROM   h
       LEFT OUTER JOIN dba_users u
       ON u.user_id = h.user_id
union all
SELECT m.sample_time, null, null
,      null, null, null, null, null, null
,      null, null, null, null, null
,      null, null, null, null, null
FROM   m
where  m.sample_time <= sysdate
)
SELECT /*NO_PARALLEL*/ sample_time, instance_number, event
,      module2 module, action, consumer_group_name
,      program1, program2, username, service_name
,      in_hard_parse, in_parse, in_sql_execution, in_plsql_execution, in_inmemory_query
,      sum(usecs_per_row)/1e6/num_secs num_sessions
,      sum(usecs_per_row)/1e6 ash_Secs
--, 10*COUNT(distinct sample_id) elap_secs
FROM x 
GROUP BY sample_time, num_secs, instance_number, event
,      module2, action, consumer_group_name
,      program1, program2, username, service_name
,      in_hard_parse, in_parse, in_sql_execution, in_plsql_execution, in_inmemory_query
ORDER BY sample_time 
/
