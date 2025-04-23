insert into stv2025011443__dwh.l_user_group_activity (
    hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src
)
select distinct
    hash(hu.hk_user_id, hg.hk_group_id)
    , hu.hk_user_id
    , hg.hk_group_id
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__staging.group_log as gl
left join stv2025011443__dwh.h_users as hu on gl.user_id = hu.user_id
left join stv2025011443__dwh.h_groups as hg on gl.group_id = hg.group_id;
