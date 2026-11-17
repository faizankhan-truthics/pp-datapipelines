{#
  Divisor that turns a Stripe or Solidgate minor-unit amount into major units for the given currency column:
  1 for zero-decimal currencies, 100 otherwise. Same list as int_customer_revenue and
  net_revenue_daily.
#}
{% macro minor_unit_divisor(currency_column) %}
    case
        when upper({{ currency_column }}) in (
            'BIF', 'CLP', 'DJF', 'GNF', 'JPY', 'KMF', 'KRW', 'MGA',
            'PYG', 'RWF', 'VND', 'VUV', 'XAF', 'XOF', 'XPF'
        ) then 1
        else 100
    end
{% endmacro %}
