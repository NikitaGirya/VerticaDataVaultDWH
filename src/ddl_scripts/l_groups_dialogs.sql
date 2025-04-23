insert into stv2025011443__dwh.l_groups_dialogs (
    hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src
)
select
    hash(hd.hk_message_id, hg.hk_group_id)
    , hd.hk_message_id
    , hg.hk_group_id
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__staging.dialogs as d
left join stv2025011443__dwh.h_dialogs as hd on d.message_id = hd.message_id
left join stv2025011443__dwh.h_groups as hg on d.message_group = hg.group_id
where d.message_group is not null;
