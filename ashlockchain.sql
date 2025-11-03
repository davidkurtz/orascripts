REM ashlockchain.sql
lock table my_ash in exclusive mode;
set serveroutput on 
clear screen
spool ashlockchain0

DECLARE
  l_lastrowcount NUMBER := 0;
BEGIN
  LOOP
    MERGE INTO my_ash u 
    USING (
    SELECT /*+LEADING(W)*/ w.dbid, w.instance_number, w.snap_id
    ,      w.sample_id, w.sample_time
    ,      w.session_id, w.session_serial#
    ,      h.blocking_inst_id, h.blocking_session, h.blocking_session_serial#, h.blocking_session_status
    FROM   my_ash w
    ,      my_ash h
    WHERE  h.dbid = w.dbid
    AND    h.snap_id = w.snap_id
    AND    h.instance_number = w.blocking_inst_id
    AND    h.session_id = w.blocking_session
    AND    h.session_serial# = w.blocking_session_serial#
    AND    (h.session_id != w.session_id
    OR     h.session_serial# != w.session_serial#)
    AND    (h.blocking_session != w.session_id
    AND    h.blocking_session_serial# != w.session_serial#)
    AND    (h.sample_id = w.sample_id AND w.blocking_inst_id = w.instance_number)
    AND    h.sample_time = w.sample_time
    AND    h.blocking_Session_status = 'VALID'
    AND    w.blocking_Session_status = 'VALID'
    ) s
    ON (   u.dbid = s.dbid
    AND    u.instance_number = s.instance_number
    AND    u.snap_id = s.snap_id
    AND    u.sample_id = s.sample_id
    AND    u.sample_time = s.sample_time
    AND    u.session_id = s.session_id
    AND    u.session_serial# = s.session_serial#)
    WHEN MATCHED THEN UPDATE
    SET u.blocking_inst_id = s.blocking_inst_id
    ,   u.blocking_session = s.blocking_session
    ,   u.blocking_session_serial# = s.blocking_session_serial#
    ,   u.blocking_session_status = s.blocking_session_status;
    dbms_output.put_line(''||SQL%ROWCOUNT||' rows merged.');
    commit;


    FOR s IN (
    SELECT /*+LEADING(W)*/ w.dbid, w.instance_number, w.snap_id
    ,      w.sample_id, w.sample_time
    ,      w.session_id, w.session_serial#
    ,      h.blocking_inst_id, h.blocking_session, h.blocking_session_serial#, h.blocking_session_status
    FROM   my_ash w
    ,      my_ash h
    WHERE  h.dbid = w.dbid
    AND    h.snap_id = w.snap_id
    AND    h.instance_number = w.blocking_inst_id
    AND    h.session_id = w.blocking_session
    AND    h.session_serial# = w.blocking_session_serial#
    AND    (h.session_id != w.session_id
    OR     h.session_serial# != w.session_serial#)
    AND    (h.blocking_session != w.session_id
    AND    h.blocking_session_serial# != w.session_serial#)
    AND    (h.sample_id = w.sample_id OR w.blocking_inst_id != w.instance_number)
    AND    h.sample_time > w.sample_time-5/86400
    AND    h.sample_time < w.sample_time+5/86400
    AND    h.blocking_Session_status = 'VALID'
    AND    w.blocking_Session_status = 'VALID'
    ) LOOP
    UPDATE my_ash u 
    SET u.blocking_inst_id = s.blocking_inst_id
    ,   u.blocking_session = s.blocking_session
    ,   u.blocking_session_serial# = s.blocking_session_serial#
    ,   u.blocking_session_status = s.blocking_session_status
    WHERE u.dbid = s.dbid
    AND    u.instance_number = s.instance_number
    AND    u.snap_id = s.snap_id
    AND    u.sample_id = s.sample_id
    AND    u.sample_time = s.sample_time
    AND    u.session_id = s.session_id
    AND    u.session_serial# = s.session_serial#;
    END LOOP;

    dbms_output.put_line(''||SQL%ROWCOUNT||' rows updated.');
    commit;

    IF SQL%ROWCOUNT=0 OR (l_lastrowcount > 0 AND SQL%ROWCOUNT = l_lastrowcount) THEN
      EXIT;
    ELSE
      l_lastrowcount := SQL%ROWCOUNT;
    END IF;
  END LOOP;
END;
/

column event format a30 word_wrapped on
column max_Sample_time format a30
column wprogram format a12 word_wrapped on
column hprogram format a12 word_wrapped on
column hmodule format a20 word_wrapped on
column wmodule format a20 word_wrapped on
column haction format a32 word_wrapped on
column waction format a32 word_wrapped on
column hruncntlid format a20 word_wrapped on
column wruncntlid format a20 word_wrapped on 
column hpi format 999999999 heading 'Holding|P.I.' word_wrapped on
column wpi format 999999999 heading 'Waiting|P.I.' word_wrapped on
column hsql_text format a160 word_wrapped on
column wsql_text format a160 word_wrapped on
set pages 999 lines 160 long 5000 trimspool on
clear screen
spool ashlockchain
with x as (
SELECT /*+LEADING(h w) USE_NL(h w)*/ 
       w.dbid, w.event
,      h.sql_id hsql_id
,      w.sql_id wsql_id
,      w.module wmodule
,      h.module hmodule
,      w.action waction
,      h.action haction
,      w.program wprogram
,      h.program hprogram
--,      w.runcntlid wruncntlid, h.runcntlid hruncntlid
--,      w.prcsinstance wpi, h.prcsinstance hpi
,      SUM(W.usecs_per_row)/1e6 ash_secs
,      max(w.sample_time) max_sample_time
FROM   my_ash w
       LEFT OUTER JOIN my_ash h
       ON        h.dbid = w.dbid
       AND       h.instance_number = w.blocking_inst_id
       AND       h.snap_id = w.snap_id
       AND        h.sample_id = w.sample_id
       AND        h.sample_time = w.sample_time
       AND        h.session_id = w.blocking_session
       AND        h.session_serial# = w.blocking_session_serial#
WHERE  w.blocking_session IS NOT NULL
And    w.instance_number = w.blocking_inst_id
GROUP BY w.dbid, w.event, h.sql_id, w.sql_id, w.module, h.module, w.action, h.action, w.program, h.program
--, h.sql_plan_hash_value
--, w.prcsinstance, h.prcsinstance
--, w.runcntlid, h.runcntlid 
UNION ALL
SELECT /*+LEADING(h w) USE_NL(h w)*/ 
       w.dbid, w.event
,      h.sql_id hsql_id
,      w.sql_id wsql_id
,      w.module wmodule
,      h.module hmodule
,      w.action waction
,      h.action haction
,      w.program wprogram
,      h.program hprogram
--,      w.prcsinstance wpi, h.prcsinstance hpi
--,      w.runcntlid wruncntlid, h.runcntlid hruncntlid
,      SUM(W.usecs_per_row)/1e6 ash_secs
,      max(w.sample_time) max_sample_time
FROM   my_ash w
       LEFT OUTER JOIN my_ash h
       ON        h.dbid = w.dbid
       AND       h.instance_number = w.blocking_inst_id
       AND       h.snap_id = w.snap_id
       AND        h.sample_time >= w.sample_time-5/86400
       AND        h.sample_time <  w.sample_time+5/86400
       AND        h.session_id = w.blocking_session
       AND        h.session_serial# = w.blocking_session_serial#
WHERE  w.blocking_session IS NOT NULL
And    w.instance_number != w.blocking_inst_id
GROUP BY w.dbid, w.event, h.sql_id, w.sql_id, w.module, h.module, w.action, h.action , w.program, h.program
--, h.sql_plan_hash_value
--, w.prcsinstance, h.prcsinstance
--,w.runcntlid, h.runcntlid 
)
select x.event
,      x.hmodule, x.haction, x.hprogram
--,      x.hruncntlid
,      x.hsql_id
,      NVL(h.sql_text,'<SQL Not Captured>') hsql_text --, hpi
,      x.max_sample_time
,      x.wmodule, x.waction, x.wprogram
--,      x.wruncntlid 
,      x.wsql_id
,      x.ash_Secs
,      NVL(w.sql_text,'<SQL Not Captured>') wsql_text --, wpi
from x
left outer join dba_Hist_sqltext h on h.dbid = x.dbid AND h.sql_id = x.hsql_id
left outer join dba_Hist_sqltext w on w.dbid = x.dbid AND w.sql_id = x.wsql_id
where wmodule = 'FS_STREAMLN' or hmodule = 'FS_STREAMLN'
ORDER BY x.ash_secs desc 
--Fetch first 100 rows only
/
spool off

