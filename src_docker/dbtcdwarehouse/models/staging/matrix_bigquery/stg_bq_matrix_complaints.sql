{{
  config(
    tags=['bq_matrix']
  )
}}

with source as (
      select * from {{ source('bq_matrix', 'complaints') }}
),
renamed as (
    select
        {{ adapter.quote("comp_action") }},
        {{ adapter.quote("comp_date") }},
        {{ adapter.quote("comp_entdate") }},
        {{ adapter.quote("comp_issue") }},
        {{ adapter.quote("comp_locid") }},
        {{ adapter.quote("comp_message_sent") }},
        {{ adapter.quote("comp_nextactdate") }},
        {{ adapter.quote("comp_perid") }},
        {{ adapter.quote("comp_reason_text") }},
        {{ adapter.quote("comp_relation") }},
        {{ adapter.quote("comp_resolved") }},
        {{ adapter.quote("comp_round") }},
        {{ adapter.quote("comp_text") }},
        {{ adapter.quote("comp_type") }},
        {{ adapter.quote("creason_id") }},
        {{ adapter.quote("csource_id") }},
        {{ adapter.quote("timestamp") }},
        {{ adapter.quote("comp_id") }},
        {{ adapter.quote("vfd_flag") }},
        {{ adapter.quote("caddr_locid") }},
        {{ adapter.quote("printed_flag") }},
        {{ adapter.quote("message_method") }},
        {{ adapter.quote("complaint_qty") }},
        {{ adapter.quote("create_sysperson") }},
        {{ adapter.quote("resolved_sysperson") }},
        {{ adapter.quote("resolved_datetime") }},
        {{ adapter.quote("business_pointer") }},
        {{ adapter.quote("sord_pointer") }},
        {{ adapter.quote("CreditExtIndicator") }},
        {{ adapter.quote("ObjectPointer") }},
        {{ adapter.quote("SourceID") }},
        {{ adapter.quote("ResolutionText") }},
        {{ adapter.quote("ProductID") }},
        {{ adapter.quote("ActivityID") }},
        {{ adapter.quote("iss_seq") }},
        {{ adapter.quote("BatchNo") }},
        {{ adapter.quote("hash_key") }},
        {{ adapter.quote("update_time") }}

    from source
)
select * from renamed
