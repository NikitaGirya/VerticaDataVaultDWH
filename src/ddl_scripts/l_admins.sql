insert into stv2025011443__dwh.l_admins (
    hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src
)
select
    hash(hg.hk_group_id, hu.hk_user_id)
    , hg.hk_group_id
    , hu.hk_user_id
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__staging.groups as g
left join stv2025011443__dwh.h_users as hu on g.admin_id = hu.user_id
left join stv2025011443__dwh.h_groups as hg on g.id = hg.group_id;
