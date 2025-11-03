REM awr_purge.sql
REM see also https://blog.go-faster.co.uk/2019/10/purging-sql-statements-and-execution.html
set pages 99 lines 200 echo on
spool awr_purge
----------------------------------------------------------------------------------------------------
--Usage: EXEC gfc_awr_purge.purge_snapshots; --to purge AWR snapshots older than retention other than for current DBID and then calls purge_orphans
--Usage: EXEC gfc_awr.purge.purge_orphans; --delete any rows from WRH$ tables with snapshots before first snap for each dbid including current DBID
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE sys.gfc_awr_purge AS 
PROCEDURE purge_orphans;
PROCEDURE purge_partitions;
PROCEDURE purge_snapshots;
END gfc_awr_purge;
/
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY sys.gfc_awr_purge AS 
----------------------------------------------------------------------------------------------------
k_module CONSTANT VARCHAR2(64) := $$PLSQL_UNIT;
-------------------------------------------------------------------------------------------------------
--print timestamped debug message
-------------------------------------------------------------------------------------------------------
PROCEDURE debug_msg(p_text VARCHAR2 DEFAULT '') IS
BEGIN
  IF p_text IS NOT NULL THEN
    $if $$debug_on $THEN
	  dbms_output.put_line('Test Mode:'||p_text);
	$else
      dbms_output.put_line(TO_CHAR(sysdate,'HH24.MI.SS')||':'||p_text);
	$end
  END IF;
END debug_msg;
----------------------------------------------------------------------------------------------------
-- purge orphaned data after snapshots have been deleted
----------------------------------------------------------------------------------------------------
PROCEDURE purge_orphan_snapshots 
(p_dbid INTEGER
,p_min_snap_id INTEGER
,p_max_snap_id INTEGER
) IS
  PRAGMA AUTONOMOUS_TRANSACTION;
  l_sql CLOB;
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
  l_rowcount INTEGER;
BEGIN
  dbms_application_info.read_module(module_name=>l_module,action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module,action_name=>'purge_orphan_snapshots');
  debug_msg('Purging dbid '||p_dbid||': orphan snapshots '||p_min_snap_id||'-'||p_max_snap_id);
  FOR j IN (
    SELECT t.owner, t.table_name, c2.low_value
    FROM   dba_tables t
    ,      dba_tab_columns c1
    ,      dba_tab_columns c2
    WHERE  t.owner = 'SYS'
    AND    t.table_name like 'WRH$%'
    AND    c1.owner = t.owner
    AND    c1.table_name = t.table_name
    AND    c1.column_name = 'DBID'
    AND    c2.owner = t.owner
    AND    c2.table_name = t.table_name
    AND    c2.column_name = 'SNAP_ID'
    ORDER BY 1,2
  ) LOOP
    dbms_application_info.set_module(module_name=>k_module,action_name=>'purge_orphan_snapshots-'||j.table_name);
    l_sql := 'DELETE FROM '||j.owner||'.'||j.table_name||' WHERE dbid = :1 AND snap_id BETWEEN :2 AND :3';
	$if $$debug_on $THEN
	  debug_msg(l_sql);
	$ELSE
      EXECUTE IMMEDIATE l_sql USING p_dbid, p_min_snap_id, p_max_snap_id;
      l_rowcount := sql%rowcount;
      IF l_rowcount > 0 THEN
        debug_msg(j.table_name||':'||l_rowcount||' rows deleted.');
      END IF;
	$END
  END LOOP;
  commit; /*because this is an autonomous transaction*/
  dbms_application_info.set_module(module_name=>l_module,action_name=>l_action);
END purge_orphan_snapshots;
----------------------------------------------------------------------------------------------------
-- drop partitions left behind after data has been purged
----------------------------------------------------------------------------------------------------
PROCEDURE purge_partitions IS
  l_this_dbid VARCHAR2(64);
  l_part_dbid VARCHAR2(64);
  l_part_snap VARCHAR2(64);
  l_part_snap_id INTEGER;
  l_pos INTEGER;
  l_sql CLOB;
  l_min_retained_snap_id INTEGER;
  l_num_rows INTEGER;
  
  TYPE t_wr_control IS TABLE OF INTEGER INDEX BY VARCHAR2(30);
  wr_control t_wr_control;
  
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
  
  e_pluggable EXCEPTION;
  PRAGMA EXCEPTION_INIT (e_pluggable, -65040);
BEGIN
  dbms_application_info.read_module(module_name=>l_module,action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module,action_name=>'purge_partitions');
  
  --dbid on current database
  SELECT dbid INTO l_this_dbid FROM v$database; 
  
  --associative array of rentention dates for each database
  FOR i IN (
    SELECT c.dbid, MIN(x.snap_id) min_retained_snap_id
    FROM   dba_hist_wr_control c
	  LEFT OUTER JOIN dba_hist_snapshot x
	  ON x.dbid = c.dbid
	  AND x.begin_interval_time >= sysdate-c.retention
	GROUP BY c.dbid
  ) LOOP
    wr_control(i.dbid) := i.min_retained_snap_id;
	debug_msg('dbid:'||i.dbid||', min retained snap ID: '||wr_control(i.dbid));
  END LOOP;
    
  FOR i IN (
    SELECT p.table_owner, p.table_name, p.partition_name, p.high_value
    FROM dba_part_key_columns k1
    ,    dba_part_key_columns k2
    ,    dba_tab_partitions p
    WHERE p.table_owner = 'SYS'
    AND p.table_name LIKE 'WRH$%'
    AND k1.owner = p.table_owner
    AND k1.object_Type = 'TABLE'
    AND k1.name = p.table_name
    AND k1.column_name = 'DBID'
    AND k1.column_position = 1
    AND k2.owner = p.table_owner
    AND k2.objecT_type = k1.object_type
    AND k2.name = p.table_name
    AND k2.column_name = 'SNAP_ID'
    AND k2.column_position = 2
	AND NOT p.partition_name like 'WRH$%MXDB_MXSN'
	AND NOT p.partition_name like 'WRH$%'||l_this_dbid||'%'
    ORDER BY 1,2,3
  ) LOOP
    l_pos := INSTR(i.high_value,',');
	l_part_dbid := SUBSTR(i.high_value,1,l_pos-1);
	l_part_snap := SUBSTR(i.high_value,l_pos+2);
	BEGIN
	  l_part_snap_id := TO_NUMBER(l_part_snap);
	EXCEPTION WHEN value_error THEN
	  l_part_snap_id := NULL;
	END;
    BEGIN
      l_min_retained_snap_id := wr_control(l_part_dbid);
    EXCEPTION WHEN no_data_found THEN
	  l_min_retained_snap_id := -1;
    END;

	--debug_msg(i.table_owner||'.'||i.table_name||' PARTITION('||i.partition_name||'):'||l_part_dbid||':'||l_part_snap);
	IF l_part_dbid = l_this_dbid THEN  
	  debug_msg('Partition '||i.partition_name||' for current database ID '||l_this_dbid||'. Take no action');
	ELSIF l_part_dbid = 'MAXVALUE' AND l_part_snap = 'MAXVALUE' THEN
	  debug_msg('Maxvalue partition '||i.partition_name||'. Take no action');
	ELSIF l_min_retained_snap_id < 0 THEN /*no control record*/
	  debug_msg('No control record for dbid '||l_part_dbid||'. Drop partition '||i.partition_name);
	  l_min_retained_snap_id := -1;
	ELSIF l_min_retained_snap_id IS NULL THEN /*no retained snaps*/
	  debug_msg('No snapshots expected for dbid '||l_part_dbid||'. Drop partition '||i.partition_name);
	  l_min_retained_snap_id := -1;
	ELSIF l_min_retained_snap_id >= l_part_snap_id THEN /*no data expected in partition*/
	  debug_msg('Min retained snapshot '||l_min_retained_snap_id||' greater than max value '||l_part_snap||'.  Drop partition '||i.partition_name);
	  l_min_retained_snap_id := -1;
	ELSE 
	  debug_msg('End of logic.  Do nothing.');
	  l_min_retained_snap_id := -1;
	END IF;
	
	IF l_min_retained_snap_id < 0 THEN
	  l_sql := 'SELECT COUNT(*) FROM '||i.table_owner||'.'||i.table_name||' PARTITION ('||i.partition_name||') WHERE ROWNUM = 1';
	  EXECUTE IMMEDIATE l_sql INTO l_num_rows;
	  debug_msg(l_sql||':'||l_num_rows);
	  IF l_num_rows > 0 THEN 
	    l_min_retained_snap_id := l_num_rows;
	  END IF;
	END IF;
	IF l_min_retained_snap_id < 0 THEN
	  l_sql := 'ALTER TABLE '||i.table_owner||'.'||i.table_name||' DROP PARTITION '||i.partition_name||' UPDATE INDEXES';
	  debug_msg(l_sql);
      $if $$debug_on $THEN 
	    debug_msg('Partition purge surpressed');
	  $else
        EXECUTE IMMEDIATE l_sql;
	  $end
	END IF;
  END LOOP;
  
  dbms_application_info.set_module(module_name=>l_module,action_name=>l_action);
END purge_partitions;
----------------------------------------------------------------------------------------------------
--delete orphaned data in WRH$ tables after snapshots have been dropped by supported APIs
----------------------------------------------------------------------------------------------------
PROCEDURE purge_orphans IS
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
BEGIN
  dbms_application_info.read_module(module_name=>l_module,action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module,action_name=>'purge_orphans');
  FOR i IN(
    SELECT x.dbid
    ,      MIN(x.snap_id) min_snap_id
    FROM   v$database d
    ,      dba_hist_wr_control c
    ,      dba_hist_snapshot x
    WHERE  c.dbid = x.dbid
    --AND    c.dbid != d.dbid
    GROUP BY x.dbid
    ORDER BY 1,2
  ) LOOP
    purge_orphan_snapshots(i.dbid,0,i.min_snap_id-1);
  END LOOP;
  dbms_application_info.set_module(module_name=>l_module,action_name=>l_action);
END purge_orphans;
----------------------------------------------------------------------------------------------------
--drop snapshots beyond defined retention by supported API - then purge orphans
----------------------------------------------------------------------------------------------------
PROCEDURE purge_snapshots IS
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
BEGIN
  dbms_application_info.read_module(module_name=>l_module,action_name=>l_action);
  dbms_application_info.set_module(module_name=>k_module,action_name=>'purge_snapshots');
  FOR i IN (
    SELECT x.dbid
    ,      MIN(x.snap_id) min_snap_id
    ,      MAX(x.snap_id) max_snap_id
    FROM   dba_hist_wr_control c
    ,      dba_hist_snapshot x
	,      v$database d
    WHERE  c.dbid = x.dbid
    AND    (  c.dbid != c.src_dbid /*an imported DBID or an old DBID*/
	       OR c.dbid != d.dbid
	       )
    AND    x.end_interval_time < sysdate-c.retention
    AND    x.begin_interval_time < TRUNC(sysdate-c.retention)
    GROUP BY x.dbid, trunc(x.begin_interval_time)
    ORDER BY 1,2
  ) LOOP
    debug_msg('Purging dbid '||i.dbid||': snapshots '||i.min_snap_id||'-'||i.max_snap_id);
	$if $$debug_on $THEN
	$ELSE
      dbms_workload_repository.drop_snapshot_range(i.min_snap_id,i.max_snap_id,i.dbid);
	$end
    purge_orphan_snapshots(i.dbid, 0, i.max_snap_id);
  END LOOP;
  gfc_awr_purge.purge_orphans;
  gfc_awr_purge.purge_partitions;
  dbms_application_info.set_module(module_name=>l_module,action_name=>l_action);
END purge_snapshots;
----------------------------------------------------------------------------------------------------
END gfc_awr_purge;
/
show errors
set serveroutput on

----------------------------------------------------------------------------------------------------
--compile in test mode - no purge occurs
----------------------------------------------------------------------------------------------------
ALTER PACKAGE sys.gfc_awr_purge COMPILE PLSQL_CCFLAGS = 'debug_on:TRUE' REUSE SETTINGS;
----------------------------------------------------------------------------------------------------
-- exec dbms_workload_repository.create_snapshot;
----------------------------------------------------------------------------------------------------
--Usage example:
set serveroutput on 
exec sys.gfc_awr_purge.purge_snapshots;
--exec sys.gfc_awr_purge.purge_orphans;
--exec sys.gfc_awr_purge.purge_partitions;
----------------------------------------------------------------------------------------------------
ALTER PACKAGE sys.gfc_awr_purge COMPILE PLSQL_CCFLAGS = 'debug_on:FALSE' REUSE SETTINGS;
----------------------------------------------------------------------------------------------------
spool OFF