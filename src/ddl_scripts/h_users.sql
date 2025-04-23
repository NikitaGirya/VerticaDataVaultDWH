insert into stv2025011443__dwh.h_users (
    hk_user_id, user_id, registration_dt, load_dt, load_src
)
select
    hash(id) as hk_user_id
    , id as user_id
    , registration_dt
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__staging.users;
