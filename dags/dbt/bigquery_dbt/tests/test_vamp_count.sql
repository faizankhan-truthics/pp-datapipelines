SELECT *
FROM {{ ref('stripe_monthly_vamp') }}
WHERE vamp_count != fraud_count + dispute_count