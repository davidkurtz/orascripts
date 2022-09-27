REM gfc_application_info.sql
clear screen 
spool gfc_application_info
show user

drop package body gfc_application_info;
drop package gfc_application_info;
-------------------------------------------------------------------------------------------------------
-- package header
-------------------------------------------------------------------------------------------------------
create or replace package gfc_application_info as 
procedure set_module /*set module to calling package and action to calling procedure and return old values through outbound parameters*/
(p_module OUT VARCHAR2 
,p_action OUT VARCHAR2 
,p_action_suffix VARCHAR2 := NULL
);

procedure set_action
(p_action_suffix VARCHAR2
);

$if $$debug_on $THEN
procedure test;
$end
end;
/

-------------------------------------------------------------------------------------------------------
--package body
-------------------------------------------------------------------------------------------------------
create or replace package body gfc_application_info as 
-------------------------------------------------------------------------------------------------------
-- set module and action automatically to package and procedure name, append suffix to action
-------------------------------------------------------------------------------------------------------
procedure set_module 
(p_module OUT VARCHAR2 
,p_action OUT VARCHAR2 
,p_action_suffix VARCHAR2 := NULL
) is
--l_dynamic_depth INTEGER;
  l_subprogram VARCHAR2(129);
  l_pos INTEGER;
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
begin
  l_subprogram := utl_call_stack.concatenate_subprogram( utl_call_stack.subprogram( 2 ));
  l_pos := INSTR(l_subprogram,'.');
  l_module := SUBSTR(l_subprogram,1,l_pos-1);
  l_action := SUBSTR(l_subprogram,l_pos+1)||p_action_suffix;
$if $$debug_on $THEN
  dbms_output.put_line(l_subprogram);
  dbms_output.put_line('module='||l_module);
  dbms_output.put_line('action='||l_action);
$end
  dbms_application_info.read_module(module_name=>p_module,action_name=>p_action);
  dbms_application_info.set_module(module_name=>l_module,action_name=>l_action);
end set_module;
-------------------------------------------------------------------------------------------------------
-- set action to procedure name and passed suffix
-------------------------------------------------------------------------------------------------------
procedure set_action
(p_action_suffix VARCHAR2
) IS
  l_subprogram VARCHAR2(129);
  l_pos INTEGER;
  l_action VARCHAR2(64);
BEGIN
  l_subprogram := utl_call_stack.concatenate_subprogram( utl_call_stack.subprogram( 2 ));
  l_pos := INSTR(l_subprogram,'.');
  l_action := SUBSTR(l_subprogram,l_pos+1)||p_action_suffix;
$if $$debug_on $THEN
  dbms_output.put_line(l_subprogram);
  dbms_output.put_line('action='||l_action);
$end
  dbms_application_info.set_action(action_name=>l_action);
END set_action;
-------------------------------------------------------------------------------------------------------
-- test procedure - only present if compiled with debug code
-------------------------------------------------------------------------------------------------------
$if $$debug_on $THEN
procedure test is
  l_module VARCHAR2(64);
  l_action VARCHAR2(64);
begin
  gfc_application_info.set_module(l_module, l_action, '_SUFFIX1');
  dbms_output.put_line('initial module='||l_module);
  dbms_output.put_line('initial action='||l_action);
  gfc_application_info.set_action('_SUFFIX2');
  dbms_application_info.set_module(l_module, l_action);
end test;
$end
-------------------------------------------------------------------------------------------------------
end gfc_application_info; 
/
-------------------------------------------------------------------------------------------------------
show errors
set serveroutput on
-------------------------------------------------------------------------------------------------------
--recompile with error
-------------------------------------------------------------------------------------------------------
ALTER PACKAGE gfc_application_info COMPILE PLSQL_CCFLAGS = 'debug_on:TRUE' REUSE SETTINGS;

SET SERVEROUTPUT ON SIZE UNLIMITED
BEGIN
  DBMS_PREPROCESSOR.print_post_processed_source (
    object_type => 'PACKAGE BODY',
    schema_name => user,
    object_name => 'GFC_APPLICATION_INFO');
END;
/
desc gfc_application_info
EXEC gfc_application_info.test;
-------------------------------------------------------------------------------------------------------
--recompile without debug code
-------------------------------------------------------------------------------------------------------
ALTER PACKAGE gfc_application_info COMPILE PLSQL_CCFLAGS = 'debug_on:FALSE' REUSE SETTINGS;
--select * from user_source where name = 'GFC_APPLICATION_INFO' order by 1,2,3;
desc gfc_application_info
EXEC gfc_application_info.test /*this will error*/;
spool off
