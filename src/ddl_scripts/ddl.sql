----==== СЛОЙ STG ====----

drop table if exists stv2025011443__staging.users;
create table stv2025011443__staging.users (
    id int primary key
    , chat_name varchar(200)
    , registration_dt timestamp
    , country varchar(200)
    , age int
)
order by
    id
segmented by
hash(id) all nodes;

drop table if exists stv2025011443__staging.groups;
create table stv2025011443__staging.groups (
    id int primary key
    , admin_id int
    , group_name varchar(100)
    , registration_dt timestamp
    , is_private boolean
)
order by
    id, admin_id
segmented by
hash(id) all nodes
partition by registration_dt::date
group by calendar_hierarchy_day(registration_dt::date, 3, 2);

drop table if exists stv2025011443__staging.dialogs;
create table stv2025011443__staging.dialogs (
    message_id int primary key
    , message_ts timestamp
    , message_from int
    , message_to int
    , message varchar(1000)
    , message_group int
)
order by
    message_id
segmented by
hash(message_id) all nodes
partition by message_ts::date
group by calendar_hierarchy_day(message_ts::date, 3, 2);

drop table if exists stv2025011443__staging.group_log;
create table stv2025011443__staging.group_log (
    group_id int primary key
    , user_id int
    , user_id_from int
    , event varchar(200)
    , datetime timestamp
)
order by
    group_id
segmented by
hash(group_id) all nodes
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2);



----==== СЛОЙ DWH ====----

--== HUB ==--

drop table if exists stv2025011443__dwh.h_users;
create table stv2025011443__dwh.h_users (
    hk_user_id bigint primary key
    , user_id int
    , registration_dt datetime
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.h_groups;
create table stv2025011443__dwh.h_groups (
    hk_group_id bigint primary key
    , group_id int
    , registration_dt datetime
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.h_dialogs;
create table stv2025011443__dwh.h_dialogs (
    hk_message_id bigint primary key
    , message_id int
    , message_ts datetime
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_message_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


--== LINK ==--

drop table if exists stv2025011443__dwh.l_user_message;
create table stv2025011443__dwh.l_user_message  (
    hk_l_user_message bigint primary key
    , hk_user_id bigint not null constraint fk_l_user_message_user references stv2025011443__dwh.h_users (hk_user_id)
    , hk_message_id bigint not null constraint fk_l_user_message_message references stv2025011443__dwh.h_dialogs (hk_message_id)
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_l_user_message all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.l_admins;
create table stv2025011443__dwh.l_admins (
    hk_l_admin_id bigint primary key
    , hk_user_id bigint not null constraint fk_l_admins_user references stv2025011443__dwh.h_users (hk_user_id)
    , hk_group_id bigint not null constraint fk_l_admins_group references stv2025011443__dwh.h_groups (hk_group_id)
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_l_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.l_groups_dialogs;
create table stv2025011443__dwh.l_groups_dialogs (
    hk_l_groups_dialogs bigint primary key
    , hk_message_id bigint not null constraint fk_l_groups_dialogs_message references stv2025011443__dwh.h_dialogs (hk_message_id)
    , hk_group_id bigint not null constraint fk_l_groups_dialogs_group references stv2025011443__dwh.h_groups (hk_group_id)
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_l_groups_dialogs all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.l_user_group_activity;
create table stv2025011443__dwh.l_user_group_activity (
    hk_l_user_group_activity bigint primary key
    , hk_user_id bigint not null constraint fk_l_user_group_activity_user references stv2025011443__dwh.h_users (hk_user_id)
    , hk_group_id bigint not null constraint fk_l_user_group_activity_group references stv2025011443__dwh.h_groups (hk_group_id)
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


--== SAT ==--

drop table if exists stv2025011443__dwh.s_admins;
create table stv2025011443__dwh.s_admins (
    hk_admin_id bigint not null constraint fk_s_admins_l_admins references stv2025011443__dwh.l_admins (hk_l_admin_id)
    , is_admin boolean
    , admin_from datetime
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_admin_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.s_user_chatinfo;
create table stv2025011443__dwh.s_user_chatinfo (
    hk_user_id bigint not null constraint fk_s_user_chatinfo_h_users references stv2025011443__dwh.h_users (hk_user_id)
    , chat_name varchar(200)
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.s_user_socdem;
create table stv2025011443__dwh.s_user_socdem (
    hk_user_id bigint not null constraint fk_s_user_socdem_h_users references stv2025011443__dwh.h_users (hk_user_id)
    , country varchar(200)
    , age int
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_user_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.s_group_name;
create table stv2025011443__dwh.s_group_name (
    hk_group_id bigint not null constraint fk_s_group_name_h_groups references stv2025011443__dwh.h_groups (hk_group_id)
    , group_name VARCHAR(100)
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.s_group_private_status;
create table stv2025011443__dwh.s_group_private_status (
    hk_group_id bigint not null constraint fk_s_group_name_h_groups references stv2025011443__dwh.h_groups (hk_group_id)
    , is_private boolean
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_group_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.s_dialog_info;
create table stv2025011443__dwh.s_dialog_info (
    hk_message_id bigint not null constraint fk_s_group_name_h_groups references stv2025011443__dwh.h_dialogs (hk_message_id)
    , message varchar(1000)
    , message_from int
    , message_to int
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_message_id all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists stv2025011443__dwh.s_auth_history;
create table stv2025011443__dwh.s_auth_history (
    hk_l_user_group_activity bigint not null constraint fk_s_auth_history_l_user_group_activity references stv2025011443__dwh.l_user_group_activity (hk_l_user_group_activity)
    , user_id_from int
    , event varchar(200)
    , event_dt timestamp
    , load_dt datetime
    , load_src varchar(20)
)
order by 
    load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);
