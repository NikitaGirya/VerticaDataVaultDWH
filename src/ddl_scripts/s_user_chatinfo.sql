insert into stv2025011443__dwh.s_user_chatinfo (
    hk_user_id, chat_name, load_dt, load_src
)
select
    hu.hk_user_id
    , u.chat_name
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__dwh.h_users as hu
left join stv2025011443__staging.users as u on hu.user_id = u.id;
