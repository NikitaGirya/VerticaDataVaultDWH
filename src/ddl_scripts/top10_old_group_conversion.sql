with user_group_messages as (
    select
        lgd.hk_group_id
        , count(distinct message_from) as cnt_users_in_group_with_messages
    from stv2025011443__dwh.s_dialog_info as sdi
    inner join stv2025011443__dwh.l_groups_dialogs as lgd
        on sdi.hk_message_id = lgd.hk_message_id
    group by lgd.hk_group_id
)
, user_group_log as (
    select
        hg.hk_group_id
        , count(distinct luga.hk_user_id) as cnt_added_users
    from stv2025011443__dwh.s_auth_history as sah
    inner join stv2025011443__dwh.l_user_group_activity as luga
        on
            sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
            and sah.event = 'add'
    inner join stv2025011443__dwh.h_groups as hg
        on luga.hk_group_id = hg.hk_group_id
    group by hg.hk_group_id, hg.registration_dt
    order by hg.registration_dt
    limit 10
)
select
    ugm.hk_group_id
    , ugl.cnt_added_users
    , ugm.cnt_users_in_group_with_messages
    , ugm.cnt_users_in_group_with_messages
    / ugl.cnt_added_users as group_conversion
from user_group_messages as ugm
inner join user_group_log as ugl
    on ugm.hk_group_id = ugl.hk_group_id
order by group_conversion desc
