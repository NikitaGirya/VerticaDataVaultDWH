insert into stv2025011443__dwh.h_dialogs (
    hk_message_id, message_id, message_ts, load_dt, load_src
)
select
    hash(message_id) as hk_message_id
    , message_id
    , message_ts
    , now() as load_dt
    , 's3' as load_src
from stv2025011443__staging.dialogs;
