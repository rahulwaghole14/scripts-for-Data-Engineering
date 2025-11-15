"""declare tables to be deleted with order and field mappings
vault must be deleted first then stage so that we don't lose
the ability to find matching records"""

# pylint: disable=all

bigquery_vault = {
    "data_deletion_table": "hexa-data-report-etl-prod.cdw_stage.data_deletion",
    "join_conditions": {
        "staging_table_field": "values_memberID",
        "data_deletion_field": "user__id",
    },
    "staging_table": {
        "name": "hexa-data-report-etl-prod.cdw_stage.qualtrics__responses",
        "fields_to_retrieve": ["responseId", "values_memberID"],
        "condition": "values_memberID != 'undefined'",
    },
    "satellite_table": {
        "name": "hexa-data-report-etl-prod.prod_dw_intermediate.sat_question_response",
        "join_field": "RESPONSE_RESPONSEID",
        "hash_field": "RESPONSE_HASH",
    },
    "deletion_order": [
        {
            "table_name": "hexa-data-report-etl-prod.prod_dw_intermediate.link_article_question_response",
            "hash_column": "RESPONSE_HASH",
        },
        {
            "table_name": "hexa-data-report-etl-prod.prod_dw_intermediate.hub_question_response",
            "hash_column": "RESPONSE_HASH",
        },
        {
            "table_name": "hexa-data-report-etl-prod.prod_dw_intermediate.sat_question_response",
            "hash_column": "RESPONSE_HASH",
        },
    ],
}

source_tables_with_keys_google = [
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.acm__print_blacklist_final",
        "match_keys": {
            "marketing_id_email": "marketing_id",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.acm__print_data_blacklist",
        "match_keys": {
            "marketing_id_email": "marketing_id",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.acm__print_data_generic",
        "match_keys": {
            "email": "email",
            "marketing_id_email": "marketing_id",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.acm__print_generic_final",
        "match_keys": {
            "email": "email",
            "marketing_id_email": "marketing_id",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.acm__print_subbenefit_final",
        "match_keys": {
            "email": "email",
            "marketing_id_email": "acm_marketing_id",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.piano__subscriptions",
        "match_keys": {
            "email": "`user`.`email`",
            "user__id": "`user`.`uid`",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.presspatron__braze_user_profiles",
        "match_keys": {
            "email": "Email_address",
            "marketing_id_email": "marketing_id",
            "marketing_id": "marketing_id",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.account_management__user_profiles",
        "match_keys": {
            "user__id": "id",
            "marketing_id_email": "marketing_id_email",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.competition_whats_got_your_goat_Form_Responses_1",
        "match_keys": {"email": "Email_Address"},
    },
    {
        "table_name": "hexa-data-report-etl-prod.press_patron_api_data.users",
        "match_keys": {"email": "email"},
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.piano__vxsubscriptionlog",
        "match_keys": {
            "user__id": "User_ID__UID_",
            "email": "User_email",
        },
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage_matrix.subscriber",
        "match_field": "subs_id",
        "requires_subs_id": True,
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage_matrix.subscription",
        "match_field": "subs_pointer",
        "requires_subs_id": True,
    },
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage_matrix.tbl_person",
        "match_keys": {
            "email": "PersContact3",
        },
        "requires_subs_id": False,
    },
    {
        "table_name": "hexa-data-report-etl-prod.prod_dw_intermediate.int_matrix__to_braze_user_profile",
        "match_keys": {
            "email": "PersContact3",
            "marketing_id": "marketing_id",
        },
        "requires_subs_id": False,
    },
    # competition entries
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.competition_whats_got_your_goat_Form_Responses_1",
        "match_keys": {"email": "Email_Address"},
        "requires_subs_id": False,
    },
    # coral
    {
        "table_name": "hexa-data-report-etl-prod.cdw_stage.coral_user",
        "match_keys": {"email": "string_field_1"},
        "requires_subs_id": False,
    },
    # presspatron
    {
        "table_name": "hexa-data-report-etl-prod.prod_dw_intermediate.int_presspatron__bq_to_braze_user_profile",
        "match_keys": {
            "email": "email",
            "marketing_id": "marketing_id",
        },
        "requires_subs_id": False,
    },
    # sentiment reactions data
    {
        "table_name": "hexa-data-report-etl-prod.sentiment_raw.reactions_data_full_list",
        "match_keys": {
            "user__id": "accountid",
        },
        "requires_subs_id": False,
    },
]
