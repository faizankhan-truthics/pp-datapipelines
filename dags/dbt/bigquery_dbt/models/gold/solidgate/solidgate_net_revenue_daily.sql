{{ config(
    tags=["solidgate", "gold", "daily"],
) }}

with net_revenue as (

    select
        accounting_date as date,
        _elt_account_name as account_name,

        case
            when _elt_account_name = 'PS_PROD_LOCATIONTOOL_LIVE'
                then 'PS_PROD_LOCATIONTOOL_LIVE'
            when _elt_account_name = 'PS_PROD_TRACELO_LIVE'
                then 'PS_PROD_TRACELO_LIVE'
            else _elt_account_name
        end as reporting_account,

        case
            when _elt_account_name = 'PS_PROD_LOCATIONTOOL_LIVE'
                then 'Location Tool'
            when _elt_account_name = 'PS_PROD_TRACELO_LIVE'
                then 'Tracelo'
            when _elt_account_name = 'QR_PROD_QGA_LIVE'
                then 'QR Generator AI'
            when _elt_account_name = 'PS_PROD_INFOCHECKER'
                then 'InfoChecker'
            when _elt_account_name = 'CHATBOX_PROD'
                then 'Chatbox AI'
            when _elt_account_name = 'QUIZ_PROD_IQCENTER_LIVE'
                then 'IQ Center'
            when _elt_account_name = 'CHATBOT_PROD_SIMPL'
                then 'Simpl AI'
            when _elt_account_name = 'PS_PROD_FINDFAST_LIVE'
                then 'FindFast'
            else _elt_account_name
        end as reporting_product,

        coalesce(
            nullif(trim(product_name), ''),
            'Unmapped'
        ) as product_name,

        sum(
            case
                when record_type_key = 'SALE'
                    then payout_amount_in_major_units
                else 0
            end
        ) as gross_revenue_usd,

        -sum(
            case
                when record_type_key in (
                    'AUTHORIZATION_FEE',
                    'CAPTURE_FEE',
                    'CHARGEBACK_FEE',
                    'CHARGEBACK_SCHEME_FEE',
                    'DISCOUNT_FEE',
                    'FRAUD_SCREENING_FEE',
                    'INTERCHANGE_FEE',
                    'REFUND_FEE',
                    'REVENUE_PROTECT_SERVICE',
                    'SCHEME_FEE',
                    'THREE_D_SECURE_FEE',
                    'VOID_FEE'
                )
                    then payout_amount_in_major_units
                else 0
            end
        ) as psp_fees_usd,

        -sum(
            case
                when record_type_key = 'REFUND'
                    then payout_amount_in_major_units
                else 0
            end
        ) as refunds_usd,

        -sum(
            case
                when record_type_key in (
                    'CHARGEBACK',
                    'CHARGEBACK_REVERSED',
                    'RDR',
                    'RDR_REVERSED'
                )
                    then payout_amount_in_major_units
                else 0
            end
        ) as disputes_usd

    from {{ source('bronze_solidgate', 'solidgate_financial_entries') }}

    where record_type_key in (
        'SALE',
        'AUTHORIZATION_FEE',
        'CAPTURE_FEE',
        'CHARGEBACK',
        'CHARGEBACK_FEE',
        'CHARGEBACK_REVERSED',
        'CHARGEBACK_SCHEME_FEE',
        'DISCOUNT_FEE',
        'FRAUD_SCREENING_FEE',
        'INTERCHANGE_FEE',
        'RDR',
        'RDR_REVERSED',
        'REFUND',
        'REFUND_FEE',
        'REVENUE_PROTECT_SERVICE',
        'SCHEME_FEE',
        'THREE_D_SECURE_FEE',
        'VOID_FEE'
    )

    group by
        1, 2, 3, 4, 5

)

select
    date,
    account_name,
    reporting_account,
    reporting_product,
    product_name,
    gross_revenue_usd,
    psp_fees_usd,
    refunds_usd,
    disputes_usd,
    gross_revenue_usd
        - psp_fees_usd
        - refunds_usd
        - disputes_usd as net_revenue_usd

from net_revenue