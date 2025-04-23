insert into stv2025011443__dwh.s_user_socdem (
    hk_user_id, country, age, load_dt, load_src
)
select
    hu.hk_user_id
    , u.country
    , u.age
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__dwh.h_users as hu
left join stv2025011443__staging.users as u on hu.user_id = u.id;
