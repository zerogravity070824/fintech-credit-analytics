{% macro format_currency_idr(column_name, decimal_places=0) %}
    {#
        Formats a numeric column as Indonesian Rupiah (IDR).
        Rounds to the specified decimal places and casts to NUMERIC.

        Usage:
            {{ format_currency_idr('loan_amount_idr') }}
            {{ format_currency_idr('loan_annuity_idr', 2) }}
    #}
    CAST(ROUND({{ column_name }}, {{ decimal_places }}) AS NUMERIC)
{% endmacro %}


{% macro safe_divide_pct(numerator, denominator, decimal_places=2) %}
    {#
        Performs a safe division and returns the result as a percentage.
        Returns NULL instead of erroring on division by zero.

        Usage:
            {{ safe_divide_pct('loan_annuity_idr', 'total_income_idr') }}
    #}
    ROUND(SAFE_DIVIDE({{ numerator }}, {{ denominator }}) * 100, {{ decimal_places }})
{% endmacro %}
