{% macro grant_access() %}

    {% if target.name == 'dev' %}

        {{ log("Executing grant_access macro for source tables in dev", info=True) }}
        {% set serviceaccountmostpopulardev = env_var("DEV_DW_ACCOUNT_SERVICE_ACCOUNT_MOST_POPULAR") %}

        {% set views_mostpopular = [
            ref('int_hexa_google_analytics__most_popular_content')
        ]
        %}

        {% set datasets_mostpopular = [
            'dev_dw_intermediate'
        ]
        %}

         {% set datasets_ga4 = [
            'analytics_456451453',
        ] %}
        {% set querys %}
            {% for dataset in datasets_ga4 %}
                {% set full_dataset_ref = "`" ~ target.project ~ "." ~ dataset ~ "`" %}
                GRANT `roles/bigquery.metadataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccountmostpopulardev }}";
                GRANT `roles/bigquery.dataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccountmostpopulardev }}";
            {% endfor %}
            {% for dataset in datasets_mostpopular %}
                {% set full_dataset_ref = "`" ~ target.project ~ "." ~ dataset ~ "`" %}
                GRANT `roles/bigquery.metadataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccountmostpopulardev }}";
            {% endfor %}
            {% for view in views_mostpopular %}
                {% set full_view_ref = "`" ~ view.project ~ "." ~ view.dataset ~ "." ~ view.identifier ~ "`" %}
                GRANT `roles/bigquery.dataViewer` ON VIEW {{ full_view_ref }} TO "{{ serviceaccountmostpopulardev }}";
            {% endfor %}
        {% endset %}

        {% do run_query(querys) %}
        {{ log('Privileges granted in dev!', info=True)}}

    {% endif %}

    {% if target.name == 'prod' %}

        {{ log("Executing grant_access macro for source tables in prod", info=True) }}

        {% set serviceaccount1 = env_var("PROD_DW_ACCOUNT_SERVICE_ACCOUNT_POWERBI_READ_ONLY")  %}
        {% set serviceaccount2 = env_var("PROD_DW_ACCOUNT_SERVICE_ACCOUNT_POWERBI")  %}
        {% set useraccount2 = env_var("PROD_DW_ACCOUNT_USER_INCITES_1")  %}
        {% set serviceaccount3 = env_var("PROD_DW_ACCOUNT_SERVICE_ACCOUNT_INCITES_2")  %}
        {% set serviceaccountmostpopular = env_var("PROD_DW_ACCOUNT_SERVICE_ACCOUNT_MOST_POPULAR")  %}
        {% set useraccount1 = env_var("PROD_DW_ACCOUNT_USER_ACCOUNT_LOOKER1")  %}

        {% set usertables = [
            source('googleadmanager', 'Line_Item_ID_Current'),
            source('googleadmanager', 'Line_Item_ID_Used'),
            source('googleadmanager', 'adwallet_data'),
            source('googleadmanager', 'default_table'),
            source('googleadmanager', 'fill_rate'),
            source('googleadmanager', 'pca_gen'),
            source('googleadmanager', 'pca_hour'),
            source('googleadmanager', 'pca_view'),
            source('googleadmanager', 'pca_view_video'),
            source('googleadmanager', 'pca_view_video_adwallet'),
            source('googleadmanager', 'vv_gen'),
            source('googleadmanager', 'vv_sellthrough'),
            source('brightcove', 'brightcove__daily_videos'),
            source('brightcove', 'brightcove__daily_videos_destination')
        ] %}

        {% set userdatasets = [
            'gam_dw_prod',
            'cdw_stage',
            'prod_dw_intermediate',
            'prod_dw_staging'
        ] %}

        {% set userviews = [
            ref('int_gam__adwallet_metrics'),
            ref('base_googleadmanager_adwallet_data'),
            ref('base_googleadmanager_pca_view_video_adwallet'),
        ] %}

        {% set tables = [
            source('piano', 'piano__vxsubscriptionlog'),
            source('piano', 'piano__vxconversionreport_conversion_page'),
            source('piano', 'piano__subscriptions'),
            source('piano', 'piano__converted_terms'),
            source('piano', 'piano__composerreport_conversion_page'),
            source('adw', 'uira_targets_Brand__'),
            source('adw', 'uira_targets_Full'),
            source('adw', 'uira_targets_Targets_Weekly'),
            source('adw', 'uira_targets_Targets'),
            source('adw', 'uira_targets_append_Brand__'),
            source('adw', 'uira_targets_append_Full'),
            source('adw', 'uira_targets_append_Targets_Weekly'),
            source('adw', 'uira_targets_append_Targets'),
            source('brightcove', 'brightcove__daily_videos'),
            source('commentary', 'digitalsubscriptions_Commentary___Editorial'),
            source('commentary', 'digitalsubscriptions_Commentary___Marketing'),
            ref('dim_article'),
            ref('dim_region'),
            ref('dim_sentiment_why'),
            ref('dim_sentiment'),
            ref('dim_tag'),
            ref('fact_sentiment'),
            ref('hub_adobe_analytics'),
            ref('sat_adobe_analytics_masthead'),
            source('drupal_dpa', 'drupal__articles'),
            source('network', 'network__datetable'),
            source('network', 'network__masthead_articles_by_day'),
            source('network', 'network__masthead_daily'),
            source('network', 'network__masthead_logged_in_users'),
            source('network', 'network__navigatableforpbi'),
            source('network', 'network__targetstableforpbi'),
            source('piano', 'piano__composerreport_conversion_page'),
            source('piano', 'piano__converted_terms'),
            source('piano', 'piano__subscriptions'),
            source('piano', 'piano__vxconversionreport_conversion_page'),
            source('piano', 'piano__vxsubscriptionlog'),
            source('googleadmanager', 'adwallet_data'),
            source('googleadmanager', 'fill_rate'),
            source('googleadmanager', 'pca_gen'),
            source('googleadmanager', 'pca_hour'),
            source('googleadmanager', 'pca_view'),
            source('googleadmanager', 'vv_gen'),
            source('googleadmanager', 'vv_sellthrough')
        ] %}

        {% set views = [
            ref('int_matrix__subscriptions'),
            ref('int_piano__subscriptions'),
            ref('int_matrix__subscriptions'),
            ref('int_piano__subscriptions')
        ] %}

        {% set datasets = [
            'adw_stage',
            'cdw_stage',
            'cdw_stage_matrix',
            'prod_dw_intermediate',
            'prod_dw_mart_ad_revenue',
            'prod_dw_mart_ad_revenue_tm1',
            'prod_dw_mart_adobe_analytics',
            'prod_dw_mart_sentiment',
            'prod_dw_staging'
        ]
        %}

        {% set views_mostpopular = [
            ref('int_hexa_google_analytics__most_popular_content')
        ]
        %}

        {% set datasets_mostpopular = [
            'prod_dw_intermediate'
        ]
        %}

        {% set datasets_ga4 = ['analytics_383411223'] %}

        {% set querys %}
            {% for dataset in datasets_ga4 %}
                {% set full_dataset_ref = "`" ~ target.project ~ "." ~ dataset ~ "`" %}
                GRANT `roles/bigquery.metadataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccount3 }}", "{{ useraccount2 }}";
                GRANT `roles/bigquery.dataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccount3 }}", "{{ useraccount2 }}";
            {% endfor %}
            {% for dataset in userdatasets %}
                {% set full_dataset_ref = "`" ~ target.project ~ "." ~ dataset ~ "`" %}
                GRANT `roles/bigquery.metadataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ useraccount1 }}";
            {% endfor %}
            {% for table in usertables %}
                {% set full_table_ref = "`" ~ table.project ~ "." ~ table.dataset ~ "." ~ table.identifier ~ "`" %}
                GRANT `roles/bigquery.dataViewer` ON TABLE {{ full_table_ref }} TO "{{ useraccount1 }}";
            {% endfor %}
            {% for view in userviews %}
                {% set full_view_ref = "`" ~ view.project ~ "." ~ view.dataset ~ "." ~ view.identifier ~ "`" %}
                GRANT `roles/bigquery.dataViewer` ON VIEW {{ full_view_ref }} TO "{{ useraccount1 }}";
            {% endfor %}
            {% for dataset in datasets %}
            {% set full_dataset_ref = "`" ~ target.project ~ "." ~ dataset ~ "`" %}
                GRANT `roles/bigquery.metadataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccount1 }}", "{{ serviceaccount2 }}";
            {% endfor %}
            {% for table in tables %}
            {% set full_table_ref = "`" ~ table.project ~ "." ~ table.dataset ~ "." ~ table.identifier ~ "`" %}
                GRANT `roles/bigquery.dataViewer` ON TABLE {{ full_table_ref }} TO "{{ serviceaccount1 }}", "{{ serviceaccount2 }}";
            {% endfor %}
            {% for view in views %}
                {% set full_view_ref = "`" ~ view.project ~ "." ~ view.dataset ~ "." ~ view.identifier ~ "`" %}
                GRANT `roles/bigquery.dataViewer` ON VIEW {{ full_view_ref }} TO "{{ serviceaccount1 }}", "{{ serviceaccount2 }}";
            {% endfor %}
            {% for dataset in datasets_mostpopular %}
                {% set full_dataset_ref = "`" ~ target.project ~ "." ~ dataset ~ "`" %}
                GRANT `roles/bigquery.metadataViewer` ON SCHEMA {{ full_dataset_ref }} TO "{{ serviceaccountmostpopular }}";
            {% endfor %}
            {% for view in views_mostpopular %}
                {% set full_view_ref = "`" ~ view.project ~ "." ~ view.dataset ~ "." ~ view.identifier ~ "`" %}
                GRANT `roles/bigquery.dataViewer` ON VIEW {{ full_view_ref }} TO "{{ serviceaccountmostpopular }}";
            {% endfor %}
        {% endset %}

        {% do run_query(querys) %}
        {{ log('Privileges granted in prod!', info=True)}}

    {% endif %}

{% endmacro %}
