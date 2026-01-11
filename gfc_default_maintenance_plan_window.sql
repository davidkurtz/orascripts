REM gfc_default_maintenance_plan_window.sql
REM alter weekday schedule for maintenance window to run for 3 hours twice per day at 5am and 8pm, and for 24 hours on weekends at 5am
REM https://docs.oracle.com/html/E25494_01/tasks004.htm#:~:text=Modifying%20a%20Maintenance%20Window,-The%20DBMS_SCHEDULER%20PL&text=DISABLE%20subprogram%20to%20disable%20the,next%20time%20the%20window%20opens.Note
REM https://docs.oracle.com/en/database/oracle/oracle-database/21/arpls/DBMS_SCHEDULER.html#GUID-D7A11F8A-8746-4815-91C4-BC8DDBA4C74A

set serveroutput on echo on pages 99 lines 150 timi on
column log_id format 99999999
column owner format a5
column resource_plan format a24
column schedule_name format a16
column schedule_type format a16
column window_name format a16
column window_Group_name format a24
column schedule_owner format a10 heading 'Schedule|Owner'
column repeat_interval format a60
column next_Start_Date format a48
column last_Start_Date format a48
column window_priority heading 'Window|Priority' format a8
column comments format a60
column job_name format a24
column job_class format a16
column status format a9
column error# format 99999
column instance_id heading 'Inst|ID' format 999
column session_id format a10
column number_of_windows heading 'Number|Windows' format 999
clear screen
spool gfc_default_maintenance_plan_window.lst

----------------------------------------------------------------------------------------------------
--current timezone settings
----------------------------------------------------------------------------------------------------
--ALTER SESSION SET TIME_ZONE = '-6:00';
SELECT systimestamp, DBTIMEZONE, DBMS_SCHEDULER.STIME, SESSIONTIMEZONE FROM DUAL;
--select * from v$TIMEZONE_names where tzname like '%Mex%';
--select systimestamp at time zone 'America/Mexico_City' from dual;
----------------------------------------------------------------------------------------------------
--get current scheduler timezone
----------------------------------------------------------------------------------------------------
declare
  l_attrib VARCHAR2(100);
begin
  dbms_Scheduler.get_scheduler_attribute('default_timezone',l_attrib);
  dbms_output.put_line(l_attrib);
end;
/
----------------------------------------------------------------------------------------------------
 
DECLARE
  --k_time_zone CONSTANT VARCHAR2(30) := ''; --do not specify timezone
  --k_time_zone CONSTANT VARCHAR2(30) := 'GMT';
  --k_time_zone CONSTANT VARCHAR2(30) := 'Europe/London';
  k_time_zone CONSTANT VARCHAR2(30) := 'America/Mexico_City';

  l_duration INTEGER;
  l_by VARCHAR2(30);
BEGIN
  FOR i IN ( --update all windows in DEFAULT_MAINTENANCE_PLAN
    select * 
    from   dba_Scheduler_Windows
    where  resource_plan = 'DEFAULT_MAINTENANCE_PLAN'
    and    regexp_like(window_name,'(MON|TUES|WEDNES|THURS|FRI|SATUR|SUN)DAY_WINDOW')
  ) LOOP

    IF regexp_like(i.window_name,'(MON|TUES|WEDNES|THURS|FRI)DAY_WINDOW') THEN 
      l_duration := 3; --duration of weekday opening
      l_by := ';byhour=5,20'; 
    ELSE
      l_duration := 24; --duration of weekend opening
      l_by := ';byhour=5'; 
    END IF;
    dbms_output.put_line('Window:'||i.owner||'.'||i.window_name||': schedule:'||l_by||' :'||l_duration||' hours');
    
    dbms_scheduler.disable --disable window, otherwise change does not apply until it next opens 
    (name      => i.owner||'.'||i.window_name
    );

    IF k_time_zone IS NOT NULL THEN --set start date with time zone, it applies to repeat interval
	dbms_scheduler.set_attribute
      (name      => i.owner||'.'||i.window_name
      ,attribute => 'START_DATE'
      ,value     => systimestamp at time zone k_time_zone
      );
    END IF;

    --set window opening duration
    dbms_scheduler.set_attribute
    (name      => i.owner||'.'||i.window_name
    ,attribute => 'DURATION'
    ,value     => numtodsinterval(l_duration, 'hour')
    );

    dbms_scheduler.set_attribute --set times that window opens
    (name      => i.owner||'.'||i.window_name
    ,attribute => 'REPEAT_INTERVAL'
    ,value     => 'freq=daily;byday='||SUBSTR(i.window_name,1,3)||l_by||';byminute=0;bysecond=0'
    );

    dbms_scheduler.enable
    (name      => i.owner||'.'||i.window_name
    );

    for j in (
      select * from dba_scheduler_window_groups
      where window_group_name IN('MAINTENANCE_WINDOW_GROUP','ORA$AT_WGRP_OS','ORA$AT_WGRP_SA','ORA$AT_WGRP_SQ')
    ) LOOP
      --add window to window group
      DBMS_SCHEDULER.add_window_group_member 
      (group_name  => j.window_group_name
      ,window_list => i.owner||'.'||i.window_name);
    END LOOP;

  END LOOP;
END;
/

select * from dba_scheduler_window_groups
where window_group_name = 'MAINTENANCE_WINDOW_GROUP'
/
select * from dba_Scheduler_wingroup_members
where window_group_name = 'MAINTENANCE_WINDOW_GROUP'
/
select * 
from dba_Scheduler_Windows
where resource_plan = 'DEFAULT_MAINTENANCE_PLAN'
order by next_Start_date
/

select log_id, log_date, owner, job_name, job_class, status, additional_info
from dba_scheduler_job_log
where owner = 'SYS' 
and job_name like 'ORA$AT_OS_OPT_SY%'
and not operation IN('DROP','ENABLE','UPDATE','COMPLETED')
order by log_date desc
fetch first 50 rows only
/
select log_id, log_date, owner, job_name, status, error#, actual_start_Date, run_duration, instance_id, session_id, cpu_used 
from dba_scheduler_job_run_details
where owner = 'SYS' 
and job_name like 'ORA$AT_OS_OPT_SY%'
order by /*run_duration desc,*/ log_date desc
fetch first 50 rows only
/

spool off
