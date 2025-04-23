insert into stv2025011443__dwh.l_user_message (
    hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src
)
select
    hash(hd.hk_message_id, hu.hk_user_id)
    , hu.hk_user_id
    , hd.hk_message_id
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__staging.dialogs as d
left join stv2025011443__dwh.h_users as hu on d.message_from = hu.user_id
left join stv2025011443__dwh.h_dialogs as hd on d.message_id = hd.message_id;
