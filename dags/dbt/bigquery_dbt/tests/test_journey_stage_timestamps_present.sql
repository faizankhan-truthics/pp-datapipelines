-- reached_stage must agree with the stage timestamps it is derived from
select product, user_key, reached_stage
from {{ ref('dim_user_journey') }}
where (reached_stage = 'customer'   and first_payment_ts is null)
   or (reached_stage = 'payment_attempted'
       and (payment_attempt_ts is null or first_payment_ts is not null))
   or (reached_stage = 'activation' and (activation_ts is null or first_payment_ts is not null
                                         or payment_attempt_ts is not null))
   or (reached_stage = 'signup'     and (signup_ts is null and anchor_type != 'app_user'))
   or (reached_stage = 'visitor'    and (signup_ts is not null or activation_ts is not null
                                         or payment_attempt_ts is not null
                                         or first_payment_ts is not null))
