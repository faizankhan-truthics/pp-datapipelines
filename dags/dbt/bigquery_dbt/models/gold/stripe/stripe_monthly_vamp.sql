{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"],
    partition_by={
        "field": "Month_",
        "data_type": "date",
        "granularity": "month"
    }
) }}

-- Full rebuild every run (not incremental): disputes and fraud warnings can
-- arrive months after the original charge, so an incremental filter scoped to
-- "recently touched" source rows would silently drop disputes/fraud tied to
-- older charges that fall outside this run's window.
WITH charges AS (

    SELECT
        b.id AS charge_id,
        b.calculated_statement_descriptor AS charge_descriptor,
        b.amount_usd AS charge_amount_usd,
        b._elt_stripe_account_id,

        -- Use the balance transaction's date for VAMP transaction month,
        -- falling back to the charge creation date when there is no
        -- balance transaction (e.g. uncaptured charges).
        DATE_TRUNC(
            b.captured_date,
            MONTH
        ) AS charge_month,

        b._elt_update_timestamp

    FROM {{ ref('int_stripe_charges') }} b

    WHERE b.card_brand = 'visa'
    and b.status = 'succeeded'
    and b.captured_date is not null
),

-- Early fraud warnings do not have their own balance transaction,
-- so there is no capture date available for the fraud warning itself.
-- Use the fraud warning creation date.
fraud_warnings AS (

    SELECT
        d.id AS fraud_id,
        b.charge_descriptor,
        b._elt_stripe_account_id,
        b.charge_amount_usd AS fraud_amount_usd,
        DATE_TRUNC(d.created_date, MONTH) AS fraud_month,
        d._elt_update_timestamp

    FROM {{ ref('int_stripe_early_fraud_warnings') }} d

    INNER JOIN charges b
        ON d.charge = b.charge_id
),

disputes AS (

    -- The dispute's own creation date is the VAMP transaction month;
    -- no need to fall back to a balance transaction date.
    SELECT
        d.id AS dispute_id,
        b.charge_descriptor,
        b._elt_stripe_account_id,
        d.amount_usd AS dispute_amount_usd,

        DATE_TRUNC(d.created_date, MONTH) AS dispute_month,

        d._elt_update_timestamp

    FROM {{ ref('int_stripe_disputes') }} d

    INNER JOIN charges b
        ON d.charge = b.charge_id

    WHERE d.rdr IS FALSE
),

charges_summary AS (

    SELECT
        _elt_stripe_account_id,
        charge_descriptor,
        charge_month AS Month_,
        COUNT(DISTINCT charge_id) AS visa_transactions,
        SUM(charge_amount_usd) AS charge_amount_usd,
        MAX(_elt_update_timestamp) AS _elt_update_timestamp

    FROM charges

    GROUP BY
        1,
        2,
        3
),

fraud_summary AS (

    SELECT
        _elt_stripe_account_id,
        charge_descriptor,
        fraud_month AS Month_,
        COUNT(DISTINCT fraud_id) AS fraud_count,
        COALESCE(SUM(fraud_amount_usd), 0) AS fraud_amount_usd,
        MAX(_elt_update_timestamp) AS _elt_update_timestamp

    FROM fraud_warnings

    GROUP BY
        1,
        2,
        3
),

disputes_summary AS (

    SELECT
        _elt_stripe_account_id,
        charge_descriptor,
        dispute_month AS Month_,
        COUNT(DISTINCT dispute_id) AS dispute_count,
        COALESCE(SUM(dispute_amount_usd), 0) AS dispute_amount_usd,
        MAX(_elt_update_timestamp) AS _elt_update_timestamp

    FROM disputes

    GROUP BY
        1,
        2,
        3
)

SELECT
    COALESCE(
        a._elt_stripe_account_id,
        f._elt_stripe_account_id,
        d._elt_stripe_account_id
    ) AS _elt_stripe_account_id,

    e.account,

    COALESCE(
        a.charge_descriptor,
        f.charge_descriptor,
        d.charge_descriptor
    ) AS charge_descriptor,

    COALESCE(
        a.Month_,
        f.Month_,
        d.Month_
    ) AS Month_,

    COALESCE(
        a.visa_transactions,
        0
    ) AS visa_transactions,

    ROUND(
        COALESCE(a.charge_amount_usd, 0),
        2
    ) AS charge_amount_usd,

    COALESCE(
        f.fraud_count,
        0
    ) AS fraud_count,

    ROUND(
        COALESCE(f.fraud_amount_usd, 0),
        2
    ) AS fraud_amount_usd,

    COALESCE(
        d.dispute_count,
        0
    ) AS dispute_count,

    ROUND(
        COALESCE(d.dispute_amount_usd, 0),
        2
    ) AS dispute_amount_usd,

    -- VAMP count = fraud + disputes
    COALESCE(f.fraud_count, 0)
        + COALESCE(d.dispute_count, 0)
        AS vamp_count,

    -- VAMP volume = fraud amount + dispute amount
    ROUND(
        COALESCE(f.fraud_amount_usd, 0)
            + COALESCE(d.dispute_amount_usd, 0),
        2
    ) AS vamp_volume,

    -- VAMP ratio = VAMP count / captured Visa transactions.
    -- Keep the raw ratio and format it as a percentage in the BI layer.
    COALESCE(
        ROUND(
            SAFE_DIVIDE(
                COALESCE(f.fraud_count, 0)
                    + COALESCE(d.dispute_count, 0),
                a.visa_transactions
            ),
            4
        ),
        0
    ) AS vamp_ratio,

    CONCAT(
        COALESCE(
            a._elt_stripe_account_id,
            f._elt_stripe_account_id,
            d._elt_stripe_account_id
        ),
        '-',
        CAST(
            COALESCE(
                a.Month_,
                f.Month_,
                d.Month_
            ) AS STRING
        ),
        '-',
        COALESCE(
            a.charge_descriptor,
            f.charge_descriptor,
            d.charge_descriptor,
            ''
        )
    ) AS unique_key,

    GREATEST(
        COALESCE(
            a._elt_update_timestamp,
            TIMESTAMP('1970-01-01')
        ),
        COALESCE(
            f._elt_update_timestamp,
            TIMESTAMP('1970-01-01')
        ),
        COALESCE(
            d._elt_update_timestamp,
            TIMESTAMP('1970-01-01')
        )
    ) AS _elt_update_timestamp

FROM charges_summary a

FULL OUTER JOIN fraud_summary f
    ON COALESCE(a._elt_stripe_account_id, '') = COALESCE(f._elt_stripe_account_id, '')
    AND COALESCE(a.charge_descriptor, '') = COALESCE(f.charge_descriptor, '')
    AND COALESCE(a.Month_, DATE '1900-01-01') = COALESCE(f.Month_, DATE '1900-01-01')

FULL OUTER JOIN disputes_summary d
    ON COALESCE(
        a._elt_stripe_account_id,
        f._elt_stripe_account_id,
        ''
    ) = COALESCE(d._elt_stripe_account_id, '')

    AND COALESCE(
        a.charge_descriptor,
        f.charge_descriptor,
        ''
    ) = COALESCE(d.charge_descriptor, '')

    AND COALESCE(
        a.Month_,
        f.Month_,
        DATE '1900-01-01'
    ) = COALESCE(d.Month_, DATE '1900-01-01')

LEFT JOIN {{ source('bronze_stripe', 'stripe_accounts') }} e
    ON COALESCE(
        a._elt_stripe_account_id,
        f._elt_stripe_account_id,
        d._elt_stripe_account_id
    ) = e.id