{% snapshot snap_dim_clients %}
    {#
        SCD Type 2 Snapshot for Client Demographics.
        
        Captures historical changes to client profiles over time.
        Use case: Tracking when a client's income type, education level,
        or housing situation changes — enabling historical risk analysis.
        
        Example: If a client's income_type changes from 'Working' to
        'Pensioner', this snapshot preserves both versions with valid_from
        and valid_to timestamps, enabling point-in-time portfolio analysis.
    #}

    {{
        config(
            target_database = var('gcp_project_id'),
            target_schema   = 'snapshots',
            unique_key      = 'client_id',
            strategy        = 'check',
            check_cols      = [
                'income_type',
                'education_level',
                'family_status',
                'housing_type',
                'total_children'
            ]
        )
    }}

    SELECT * FROM {{ ref('dim_clients') }}

{% endsnapshot %}
