SELECT *
FROM {{ ref('stripe_monthly_vamp') }}
WHERE ABS(
    vamp_ratio -
    round(SAFE_DIVIDE(vamp_count, visa_transactions),2)
) > 0.01