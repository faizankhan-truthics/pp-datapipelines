{{ config(
    tags=["stripe", "solidgate", "revenue", "gold", "daily"],
) }}

with solidgate_net_revenue as (

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

        -- Count Solidgate sales only when Stripe is NOT the processor.
        -- Stripe-processed sales are counted from the Stripe financial ledger.
        sum(
            case
                when record_type_key = 'SALE'
                    and upper(coalesce(provider, '')) != 'STRIPE'
                    then payout_amount_in_major_units
                else 0
            end
        ) as gross_revenue_usd,

        -- Keep ALL Solidgate fees, including fees related to
        -- transactions processed through Stripe.
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

        -- Stripe-processed refunds are counted from Stripe.
        -sum(
            case
                when record_type_key = 'REFUND'
                    and upper(coalesce(provider, '')) != 'STRIPE'
                    then payout_amount_in_major_units
                else 0
            end
        ) as refunds_usd,

        -- Stripe-processed disputes are counted from Stripe.
        -sum(
            case
                when record_type_key in (
                    'CHARGEBACK',
                    'CHARGEBACK_REVERSED',
                    'RDR',
                    'RDR_REVERSED'
                )
                    and upper(coalesce(provider, '')) != 'STRIPE'
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

),

stripe_base as (

    select
        bt.*,

        coalesce(
            nullif(trim(a.account), ''),
            bt._elt_stripe_account_id
        ) as stripe_account_name,

        case
            when upper(trim(a.account)) = 'CHAT-BOX'
                then 'Chatbox AI'

            when upper(trim(a.account)) = 'FILES-EDITOR'
                or starts_with(upper(trim(a.account)), 'FE-')
                then 'Files Editor'

            when upper(trim(a.account)) = 'INFOCHECKER'
                or starts_with(upper(trim(a.account)), 'IC-')
                then 'InfoChecker'

            when upper(trim(a.account)) = 'IQ CENTER'
                then 'IQ Center'

            when upper(trim(a.account)) = 'LT'
                then 'Location Tool'

            when upper(trim(a.account)) in ('OQG', 'OQG LLC')
                then 'Online QR Generator'

            when upper(trim(a.account)) = 'PUBLICREGISTRY.US'
                then 'Public Registry'

            when upper(trim(a.account)) = 'QCI'
                then 'QCI'

            when upper(trim(a.account)) = 'QGA'
                then 'QR Generator AI'

            when upper(trim(a.account)) = 'SIMPL.AI'
                then 'Simpl AI'

            when upper(trim(a.account)) in ('TRC', 'TRACELO')
                then 'Tracelo'

            else coalesce(
                nullif(trim(a.account), ''),
                bt._elt_stripe_account_id
            )
        end as stripe_reporting_product,

        case
            when upper(bt.currency) in (
                'BIF',
                'CLP',
                'DJF',
                'GNF',
                'JPY',
                'KMF',
                'KRW',
                'MGA',
                'PYG',
                'RWF',
                'VND',
                'VUV',
                'XAF',
                'XOF',
                'XPF'
            )
                then 1
            else 100
        end as currency_divisor

    from {{ ref('int_stripe_balance_transactions') }} bt

    left join {{ source('bronze_stripe', 'stripe_accounts') }} a
        on bt._elt_stripe_account_id = a.id

    where bt.reporting_category in (
        'charge',
        'refund',
        'refund_failure',
        'dispute',
        'dispute_reversal',
        'fee',
        'network_cost'
    )

),

stripe_net_revenue as (

    select
        created_date as date,

        stripe_account_name as account_name,

        stripe_account_name as reporting_account,

        stripe_reporting_product as reporting_product,

        stripe_reporting_product as product_name,

        sum(
            case
                when reporting_category = 'charge'
                    then coalesce(amount_usd, 0) / currency_divisor
                else 0
            end
        ) as gross_revenue_usd,

        sum(
            case
                when reporting_category in (
                    'fee',
                    'network_cost'
                )
                    then -coalesce(amount_usd, 0) / currency_divisor

                when coalesce(fee_local, 0) != 0
                    then (
                        coalesce(amount_usd, 0)
                        - coalesce(net_usd, 0)
                    ) / currency_divisor

                else 0
            end
        ) as psp_fees_usd,

        -sum(
            case
                when reporting_category in (
                    'refund',
                    'refund_failure'
                )
                    then coalesce(amount_usd, 0) / currency_divisor
                else 0
            end
        ) as refunds_usd,

        -sum(
            case
                when reporting_category in (
                    'dispute',
                    'dispute_reversal'
                )
                    then coalesce(amount_usd, 0) / currency_divisor
                else 0
            end
        ) as disputes_usd

    from stripe_base

    group by
        1, 2, 3, 4, 5

),

unified_revenue as (

    select
        date,
        account_name,
        reporting_account,
        reporting_product,
        product_name,
        gross_revenue_usd,
        psp_fees_usd,
        refunds_usd,
        disputes_usd
    from solidgate_net_revenue

    union all

    select
        date,
        account_name,
        reporting_account,
        reporting_product,
        product_name,
        gross_revenue_usd,
        psp_fees_usd,
        refunds_usd,
        disputes_usd
    from stripe_net_revenue

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

from unified_revenue
