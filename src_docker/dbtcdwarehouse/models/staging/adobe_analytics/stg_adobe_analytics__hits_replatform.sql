{{
  config(
    tags = ['stage_adobe']
  )
}}

{% set table_name = check_sat_table_exists() %}

{% set latest = get_table_suffix(table_name) %}

WITH source AS (
    SELECT *, _TABLE_SUFFIX FROM {{ source('adobe_analytics_replatform', 'data_*') }}

    {# default behavior for dev #}
    {% if (target.dataset != 'prod') and (flags.FULL_REFRESH) %}
    WHERE _TABLE_SUFFIX >= '2023120607' AND _TABLE_SUFFIX <= '2023120608'
    {% elif target.dataset != 'prod' %}
    WHERE _TABLE_SUFFIX >= '2023120607' AND _TABLE_SUFFIX <= '2023120608'

    {# production behavior #}
    {# 1. create the stage/vault/marts all together 'dbt run --select +tag:adobe' #}
    {# 2. full refresh mart only 'dbt run --select tag:adobe --full-refresh' notice removed +/plus sign #}
    {# 3. full refresh vault only 'dbt run --select +tag:vault_adobe --full-refresh' notice removed +/plus sign #}
    {# 4. NOTE do not run a combination of +tag:adobe and --full-refresh it will cause data issues in vault #}

    {% elif flags.FULL_REFRESH %}
    {% else %}
    WHERE _TABLE_SUFFIX > FORMAT_TIMESTAMP('%Y%m%d%H', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
    {% endif %}
),
referrer_type AS (
    SELECT id, any_value(name) as NAME FROM {{ source('adobe_analytics_replatform', 'referrer_type') }} group by 1
),
event_lookup AS (
    SELECT id, any_value(name) as NAME  FROM {{ source('adobe_analytics_replatform', 'event') }} group by 1
),




 renamed as (
    select
        CONCAT(HITID_HIGH, '|',  HITID_LOW, '|', VISIT_NUM, '|', VISIT_PAGE_NUM) AS ADOBE_KEY,
        SAFE_CAST({{ adapter.quote("date_time") }} AS DATETIME) AS DATE_TIME,
        {# {{ adapter.quote("accept_language") }},
        {{ adapter.quote("adclassificationcreative") }},
        {{ adapter.quote("adload") }},
        {{ adapter.quote("aemassetid") }},
        {{ adapter.quote("aemassetsource") }},
        {{ adapter.quote("aemclickedassetid") }},
        {{ adapter.quote("amo_ef_id") }}, #}
        {{ adapter.quote("browser") }} as BROWSER, --replatform
        {# {{ adapter.quote("browser_height") }},
        {{ adapter.quote("browser_width") }},
        {{ adapter.quote("c_color") }},
        {{ adapter.quote("campaign") }},
        {{ adapter.quote("carrier") }},
        {{ adapter.quote("ch_hdr") }},
        {{ adapter.quote("ch_js") }}, #}
        {{ adapter.quote("channel") }} AS CHANNEL, ----replatform
        {# {{ adapter.quote("click_action") }},
        {{ adapter.quote("click_action_type") }},
        {{ adapter.quote("click_context") }},
        {{ adapter.quote("click_context_type") }},
        {{ adapter.quote("click_sourceid") }},
        {{ adapter.quote("click_tag") }},
        {{ adapter.quote("clickmaplink") }},
        {{ adapter.quote("clickmaplinkbyregion") }},
        {{ adapter.quote("clickmappage") }},
        {{ adapter.quote("clickmapregion") }},
        {{ adapter.quote("code_ver") }},
        {{ adapter.quote("color") }},
        {{ adapter.quote("connection_type") }},
        {{ adapter.quote("cookies") }}, #}
        {{ adapter.quote("country") }} AS COUNTRY, ---replatform
        {# {{ adapter.quote("ct_connect_type") }},
        {{ adapter.quote("curr_factor") }},
        {{ adapter.quote("curr_rate") }},
        {{ adapter.quote("currency") }},
        {{ adapter.quote("cust_hit_time_gmt") }},
        {{ adapter.quote("cust_visid") }},
        {{ adapter.quote("daily_visitor") }},
        {{ adapter.quote("dataprivacyconsentoptin") }},
        {{ adapter.quote("dataprivacyconsentoptout") }}, #}
        {# {{ adapter.quote("date_time") }} AS DATE_TIME,--masthead #}
        {# {{ adapter.quote("domain") }},
        {{ adapter.quote("duplicate_events") }}, #}
        {{ adapter.quote("duplicate_purchase") }} AS DUPLICATE_PURCHASE,  --masthead
        {# {{ adapter.quote("duplicated_from") }},
        {{ adapter.quote("ef_id") }},
        {{ adapter.quote("evar1") }} AS EVAR1, --replatform
        {{ adapter.quote("evar2") }}  AS EVAR2, --replatform
        {{ adapter.quote("evar3") }}  AS EVAR3, --replatform
        {{ adapter.quote("evar4") }}  AS EVAR4, --replatform
        {{ adapter.quote("evar5") }}  AS EVAR5, --replatform
        {{ adapter.quote("evar6") }}  AS EVAR6, --replatform
        {{ adapter.quote("evar7") }}  AS EVAR7, --replatform
        {{ adapter.quote("evar8") }}  AS EVAR8, --replatform
        {{ adapter.quote("evar9") }}  AS EVAR9, --replatform
        {{ adapter.quote("evar10") }} AS EVAR10, --replatform
        {{ adapter.quote("evar11") }} AS EVAR11, --replatform
        {{ adapter.quote("evar12") }} AS EVAR12, --replatform
        {{ adapter.quote("evar13") }} AS EVAR13, --replatform
        {{ adapter.quote("evar14") }} AS EVAR14, --replatform
        {{ adapter.quote("evar15") }} AS EVAR15, --replatform
        {{ adapter.quote("evar16") }} AS EVAR16, --replatform
        {{ adapter.quote("evar17") }} AS EVAR17, --replatform
        {{ adapter.quote("evar18") }} AS EVAR18, --replatform
        {{ adapter.quote("evar19") }} AS EVAR19, --replatform
        {{ adapter.quote("evar20") }} AS EVAR20, --replatform
        {{ adapter.quote("evar21") }},
        {{adapter.quote("evar22") }},
        {{ adapter.quote("evar23") }},
        {{ adapter.quote("evar24") }},
        {{ adapter.quote("evar25") }},
        {{ adapter.quote("evar26") }},
        {{ adapter.quote("evar27") }},
        {{ adapter.quote("evar28") }},
        {{ adapter.quote("evar29") }},
        {{ adapter.quote("evar30") }},
        {{ adapter.quote("evar31") }},
        {{ adapter.quote("evar32") }},
        {{ adapter.quote("evar33") }},
        {{ adapter.quote("evar34") }},
        {{ adapter.quote("evar35") }},
        {{ adapter.quote("evar36") }},
        {{ adapter.quote("evar37") }},
        {{ adapter.quote("evar38") }},
        {{ adapter.quote("evar39") }},
        {{ adapter.quote("evar40") }},
        {{ adapter.quote("evar41") }},
        {{ adapter.quote("evar42") }},
        {{ adapter.quote("evar43") }},
        {{ adapter.quote("evar44") }},
        {{ adapter.quote("evar45") }},
        {{ adapter.quote("evar46") }},
        {{ adapter.quote("evar47") }},
        {{ adapter.quote("evar48") }},
        {{ adapter.quote("evar49") }},
        {{ adapter.quote("evar50") }},
        {{ adapter.quote("evar51") }},
        {{ adapter.quote("evar52") }},
        {{ adapter.quote("evar53") }},
        {{ adapter.quote("evar54") }},
        {{ adapter.quote("evar55") }},
        {{ adapter.quote("evar56") }},
        {{ adapter.quote("evar57") }},
        {{ adapter.quote("evar58") }},
        {{ adapter.quote("evar59") }},
        {{ adapter.quote("evar60") }},
        {{ adapter.quote("evar61") }},
        {{ adapter.quote("evar62") }},
        {{ adapter.quote("evar63") }},
        {{ adapter.quote("evar64") }},
        {{ adapter.quote("evar65") }},
        {{ adapter.quote("evar66") }},
        {{ adapter.quote("evar67") }},
        {{ adapter.quote("evar68") }},
        {{ adapter.quote("evar69") }},
        {{ adapter.quote("evar70") }},
        {{ adapter.quote("evar71") }},
        {{ adapter.quote("evar72") }},
        {{ adapter.quote("evar73") }},
        {{ adapter.quote("evar74") }},
        {{ adapter.quote("evar75") }},
        {{ adapter.quote("evar76") }},
        {{ adapter.quote("evar77") }},
        {{ adapter.quote("evar78") }},
        {{ adapter.quote("evar79") }},
        {{ adapter.quote("evar80") }},
        {{ adapter.quote("evar81") }},
        {{ adapter.quote("evar82") }},
        {{ adapter.quote("evar83") }},
        {{ adapter.quote("evar84") }},
        {{ adapter.quote("evar85") }},
        {{ adapter.quote("evar86") }},
        {{ adapter.quote("evar87") }},
        {{ adapter.quote("evar88") }},
        {{ adapter.quote("evar89") }},
        {{ adapter.quote("evar90") }},
        {{ adapter.quote("evar91") }},
        {{ adapter.quote("evar92") }},
        {{ adapter.quote("evar93") }},
        {{ adapter.quote("evar94") }},
        {{ adapter.quote("evar95") }},
        {{ adapter.quote("evar96") }},
        {{ adapter.quote("evar97") }},
        {{ adapter.quote("evar98") }},
        {{ adapter.quote("evar99") }},
        {{ adapter.quote("evar100") }},
        {{ adapter.quote("evar101") }},
        {{ adapter.quote("evar102") }},
        {{ adapter.quote("evar103") }},
        {{ adapter.quote("evar104") }},
        {{ adapter.quote("evar105") }},
        {{ adapter.quote("evar106") }},
        {{ adapter.quote("evar107") }},
        {{ adapter.quote("evar108") }},
        {{ adapter.quote("evar109") }},
        {{ adapter.quote("evar110") }},
        {{ adapter.quote("evar111") }},
        {{ adapter.quote("evar112") }},
        {{ adapter.quote("evar113") }},
        {{ adapter.quote("evar114") }},
        {{ adapter.quote("evar115") }},
        {{ adapter.quote("evar116") }},
        {{ adapter.quote("evar117") }},
        {{ adapter.quote("evar118") }},
        {{ adapter.quote("evar119") }},
        {{ adapter.quote("evar120") }},
        {{ adapter.quote("evar121") }},
        {{ adapter.quote("evar122") }},
        {{ adapter.quote("evar123") }},
        {{ adapter.quote("evar124") }},
        {{ adapter.quote("evar125") }},
        {{ adapter.quote("evar126") }},
        {{ adapter.quote("evar127") }},
        {{ adapter.quote("evar128") }},
        {{ adapter.quote("evar129") }},
        {{ adapter.quote("evar130") }},
        {{ adapter.quote("evar131") }},
        {{ adapter.quote("evar132") }},
        {{ adapter.quote("evar133") }},
        {{ adapter.quote("evar134") }},
        {{ adapter.quote("evar135") }},
        {{ adapter.quote("evar136") }},
        {{ adapter.quote("evar137") }},
        {{ adapter.quote("evar138") }},
        {{ adapter.quote("evar139") }},
        {{ adapter.quote("evar140") }},
        {{ adapter.quote("evar141") }},
        {{ adapter.quote("evar142") }},
        {{ adapter.quote("evar143") }},
        {{ adapter.quote("evar144") }},
        {{ adapter.quote("evar145") }},
        {{ adapter.quote("evar146") }},
        {{ adapter.quote("evar147") }},
        {{ adapter.quote("evar148") }},
        {{ adapter.quote("evar149") }},
        {{ adapter.quote("evar150") }},
        {{ adapter.quote("evar151") }},
        {{ adapter.quote("evar152") }},
        {{ adapter.quote("evar153") }},
        {{ adapter.quote("evar154") }},
        {{ adapter.quote("evar155") }},
        {{ adapter.quote("evar156") }},
        {{ adapter.quote("evar157") }},
        {{ adapter.quote("evar158") }},
        {{ adapter.quote("evar159") }},
        {{ adapter.quote("evar160") }},
        {{ adapter.quote("evar161") }},
        {{ adapter.quote("evar162") }},
        {{ adapter.quote("evar163") }},
        {{ adapter.quote("evar164") }},
        {{ adapter.quote("evar165") }},
        {{ adapter.quote("evar166") }},
        {{ adapter.quote("evar167") }},
        {{ adapter.quote("evar168") }},
        {{ adapter.quote("evar169") }},
        {{ adapter.quote("evar170") }},
        {{ adapter.quote("evar171") }},
        {{ adapter.quote("evar172") }},
        {{ adapter.quote("evar173") }},
        {{ adapter.quote("evar174") }},
        {{ adapter.quote("evar175") }},
        {{ adapter.quote("evar176") }},
        {{ adapter.quote("evar177") }},
        {{ adapter.quote("evar178") }},
        {{ adapter.quote("evar179") }},
        {{ adapter.quote("evar180") }},
        {{ adapter.quote("evar181") }},
        {{ adapter.quote("evar182") }},
        {{ adapter.quote("evar183") }},
        {{ adapter.quote("evar184") }},
        {{ adapter.quote("evar185") }},
        {{ adapter.quote("evar186") }},
        {{ adapter.quote("evar187") }},
        {{ adapter.quote("evar188") }},
        {{ adapter.quote("evar189") }},
        {{ adapter.quote("evar190") }},
        {{ adapter.quote("evar191") }},
        {{ adapter.quote("evar192") }},
        {{ adapter.quote("evar193") }},
        {{ adapter.quote("evar194") }},
        {{ adapter.quote("evar195") }},
        {{ adapter.quote("evar196") }},
        {{ adapter.quote("evar197") }},
        {{ adapter.quote("evar198") }},
        {{ adapter.quote("evar199") }},
        {{ adapter.quote("evar200") }},
        {{ adapter.quote("evar201") }},
        {{ adapter.quote("evar202") }},
        {{ adapter.quote("evar203") }},
        {{ adapter.quote("evar204") }},
        {{ adapter.quote("evar205") }},
        {{ adapter.quote("evar206") }},
        {{ adapter.quote("evar207") }},
        {{ adapter.quote("evar208") }},
        {{ adapter.quote("evar209") }},
        {{ adapter.quote("evar210") }},
        {{ adapter.quote("evar211") }},
        {{ adapter.quote("evar212") }},
        {{ adapter.quote("evar213") }},
        {{ adapter.quote("evar214") }},
        {{ adapter.quote("evar215") }},
        {{ adapter.quote("evar216") }},
        {{ adapter.quote("evar217") }},
        {{ adapter.quote("evar218") }},
        {{ adapter.quote("evar219") }},
        {{ adapter.quote("evar220") }},
        {{ adapter.quote("evar221") }},
        {{ adapter.quote("evar222") }},
        {{ adapter.quote("evar223") }},
        {{ adapter.quote("evar224") }},
        {{ adapter.quote("evar225") }},
        {{ adapter.quote("evar226") }},
        {{ adapter.quote("evar227") }},
        {{ adapter.quote("evar228") }},
        {{ adapter.quote("evar229") }},
        {{ adapter.quote("evar230") }},
        {{ adapter.quote("evar231") }},
        {{ adapter.quote("evar232") }},
        {{ adapter.quote("evar233") }},
        {{ adapter.quote("evar234") }},
        {{ adapter.quote("evar235") }},
        {{ adapter.quote("evar236") }},
        {{ adapter.quote("evar237") }},
        {{ adapter.quote("evar238") }},
        {{ adapter.quote("evar239") }},
        {{ adapter.quote("evar240") }},
        {{ adapter.quote("evar241") }},
        {{ adapter.quote("evar242") }},
        {{ adapter.quote("evar243") }},
        {{ adapter.quote("evar244") }},
        {{ adapter.quote("evar245") }},
        {{ adapter.quote("evar246") }},
        {{ adapter.quote("evar247") }},
        {{ adapter.quote("evar248") }},
        {{ adapter.quote("evar249") }},
        {{ adapter.quote("evar250") }}, #}
        {{ adapter.quote("event_list") }}  AS EVENT_LIST, --masthead
        {# {{ adapter.quote("exclude_hit") }}, #}
        {{ adapter.quote("first_hit_page_url") }} AS FIRST_HIT_PAGE_URL,--masthead
        {{ adapter.quote("first_hit_pagename") }} AS FIRST_HIT_PAGENAME , --masthead
        {{ adapter.quote("first_hit_ref_domain") }} AS FIRST_HIT_REF_DOMAIN, --masthead
        {{ adapter.quote("first_hit_ref_type") }} AS FIRST_HIT_REF_TYPE,--masthead
        {{ adapter.quote("first_hit_referrer") }} AS FIRST_HIT_REFERRER,--masthead
        {{ adapter.quote("first_hit_time_gmt") }} AS FIRST_HIT_TIME_GMT, --masthead
        {{ adapter.quote("geo_city") }} AS GEO_CITY,  --masthead
        {{ adapter.quote("geo_country") }} AS GEO_COUNTRY, --masthead
        {# {{ adapter.quote("geo_dma") }}, #}
        {{ adapter.quote("geo_region") }} AS GEO_REGION, --masthead
        {# {{ adapter.quote("geo_zip") }},
        {{ adapter.quote("hier1") }},
        {{ adapter.quote("hier2") }},
        {{ adapter.quote("hier3") }},
        {{ adapter.quote("hier4") }},
        {{ adapter.quote("hier5") }},
        {{ adapter.quote("hit_source") }},
        {{ adapter.quote("hit_time_gmt") }}, #}
        {{ adapter.quote("hitid_high") }}  AS HITID_HIGH, --masthead
        {{ adapter.quote("hitid_low") }}  AS HITID_LOW, --masthead
        {# {{ adapter.quote("homepage") }},
        {{ adapter.quote("hourly_visitor") }},
        {{ adapter.quote("ip") }},
        {{ adapter.quote("ip2") }},
        {{ adapter.quote("ipv6") }},
        {{ adapter.quote("j_jscript") }},
        {{ adapter.quote("java_enabled") }},
        {{ adapter.quote("javascript") }},
        {{ adapter.quote("language") }}, #}
        {{ adapter.quote("last_hit_time_gmt") }} AS LAST_HIT_TIME_GMT,  --masthead
        {# {{ adapter.quote("last_purchase_num") }},
        {{ adapter.quote("last_purchase_time_gmt") }},
        {{ adapter.quote("latlon1") }},
        {{ adapter.quote("latlon23") }},
        {{ adapter.quote("latlon45") }},
        {{ adapter.quote("mc_audiences") }}, #}
        {{ adapter.quote("mcvisid") }} AS MCVISID,  --masthead
        {{ adapter.quote("mobile_id") }} AS MOBLIE_ID,  --masthead
        {# {{ adapter.quote("mobileacquisitionclicks") }},
        {{ adapter.quote("mobileaction") }},
        {{ adapter.quote("mobileactioninapptime") }},
        {{ adapter.quote("mobileactiontotaltime") }},
        {{ adapter.quote("mobileappid") }},
        {{ adapter.quote("mobileappperformanceaffectedusers") }},
        {{ adapter.quote("mobileappperformanceappid") }},
        {{ adapter.quote("mobileappperformanceappid_app_perf_app_name") }},
        {{ adapter.quote("mobileappperformanceappid_app_perf_platform") }},
        {{ adapter.quote("mobileappperformancecrashes") }},
        {{ adapter.quote("mobileappperformancecrashid") }},
        {{ adapter.quote("mobileappperformancecrashid_app_perf_crash_name") }},
        {{ adapter.quote("mobileappperformanceloads") }},
        {{ adapter.quote("mobileappstoreavgrating") }}, #}
        {{ adapter.quote("mobileappstoredownloads") }} as MOBILE_APP_STORE_DOWNLOADS, --repltform
        {# {{ adapter.quote("mobileappstoreinapprevenue") }},
        {{ adapter.quote("mobileappstoreinapproyalties") }},
        {{ adapter.quote("mobileappstoreobjectid") }},
        {{ adapter.quote("mobileappstoreobjectid_app_store_user") }},
        {{ adapter.quote("mobileappstoreobjectid_application_name") }},
        {{ adapter.quote("mobileappstoreobjectid_application_version") }},
        {{ adapter.quote("mobileappstoreobjectid_appstore_name") }},
        {{ adapter.quote("mobileappstoreobjectid_category_name") }},
        {{ adapter.quote("mobileappstoreobjectid_country_name") }}, #}
        {{ adapter.quote("mobileappstoreobjectid_device_manufacturer") }} AS DEVICE_MANUFACTURER, --replatform
        {{ adapter.quote("mobileappstoreobjectid_device_name") }} AS DEVICE_NAME, --replatform
        {# {{ adapter.quote("mobileappstoreobjectid_in_app_name") }},
        {{ adapter.quote("mobileappstoreobjectid_platform_name_version") }},
        {{ adapter.quote("mobileappstoreobjectid_rank_category_type") }},
        {{ adapter.quote("mobileappstoreobjectid_region_name") }},
        {{ adapter.quote("mobileappstoreobjectid_review_comment") }},
        {{ adapter.quote("mobileappstoreobjectid_review_title") }},
        {{ adapter.quote("mobileappstoreoneoffrevenue") }},
        {{ adapter.quote("mobileappstoreoneoffroyalties") }},
        {{ adapter.quote("mobileappstorepurchases") }},
        {{ adapter.quote("mobileappstorerank") }},
        {{ adapter.quote("mobileappstorerankdivisor") }},
        {{ adapter.quote("mobileappstorerating") }},
        {{ adapter.quote("mobileappstoreratingdivisor") }},
        {{ adapter.quote("mobileavgprevsessionlength") }},
        {{ adapter.quote("mobilebeaconmajor") }},
        {{ adapter.quote("mobilebeaconminor") }},
        {{ adapter.quote("mobilebeaconproximity") }},
        {{ adapter.quote("mobilebeaconuuid") }},
        {{ adapter.quote("mobilecampaigncontent") }},
        {{ adapter.quote("mobilecampaignmedium") }},
        {{ adapter.quote("mobilecampaignname") }},
        {{ adapter.quote("mobilecampaignsource") }},
        {{ adapter.quote("mobilecampaignterm") }},
        {{ adapter.quote("mobilecrashes") }},
        {{ adapter.quote("mobilecrashrate") }},
        {{ adapter.quote("mobiledailyengagedusers") }}, #}
        {{ adapter.quote("mobiledayofweek") }} AS MOBILE_DAY_OF_WEEK,  --replatform
        {# {{ adapter.quote("mobiledayssincefirstuse") }},
        {{ adapter.quote("mobiledayssincelastupgrade") }}, #}
        {{ adapter.quote("mobiledayssincelastuse") }} AS MOBILE_DAYS_SINCE_LAST_USE,
        {# {{ adapter.quote("mobiledeeplinkid") }},
        {{ adapter.quote("mobiledeeplinkid_name") }}, #}
        {{ adapter.quote("mobiledevice") }} AS MOBILEDEVICE, ---replatform
        {{ adapter.quote("mobilehourofday") }} AS MOBILE_HOUR_OF_DAY, --replatform
        {# {{ adapter.quote("mobileinstalldate") }},
        {{ adapter.quote("mobileinstalls") }},
        {{ adapter.quote("mobilelaunches") }},
        {{ adapter.quote("mobilelaunchessincelastupgrade") }},
        {{ adapter.quote("mobilelaunchnumber") }},
        {{ adapter.quote("mobileltv") }},
        {{ adapter.quote("mobileltvtotal") }},
        {{ adapter.quote("mobilemessagebuttonname") }},
        {{ adapter.quote("mobilemessageclicks") }},
        {{ adapter.quote("mobilemessageid") }},
        {{ adapter.quote("mobilemessageid_dest") }},
        {{ adapter.quote("mobilemessageid_name") }},
        {{ adapter.quote("mobilemessageid_type") }},
        {{ adapter.quote("mobilemessageimpressions") }},
        {{ adapter.quote("mobilemessageonline") }},
        {{ adapter.quote("mobilemessagepushoptin") }},
        {{ adapter.quote("mobilemessagepushpayloadid") }},
        {{ adapter.quote("mobilemessagepushpayloadid_name") }},
        {{ adapter.quote("mobilemessageviews") }},
        {{ adapter.quote("mobilemonthlyengagedusers") }},
        {{ adapter.quote("mobileosenvironment") }},
        {{ adapter.quote("mobileosversion") }},
        {{ adapter.quote("mobileplaceaccuracy") }},
        {{ adapter.quote("mobileplacecategory") }},
        {{ adapter.quote("mobileplacedwelltime") }},
        {{ adapter.quote("mobileplaceentry") }}, #}
        {{ adapter.quote("mobileplaceexit") }} AS MOBILE_PLACE_EXIT, --replatform
        {# {{ adapter.quote("mobileplaceid") }},
        {{ adapter.quote("mobileprevsessionlength") }},
        {{ adapter.quote("mobilepushoptin") }},
        {{ adapter.quote("mobilepushpayloadid") }},
        {{ adapter.quote("mobilerelaunchcampaigncontent") }},
        {{ adapter.quote("mobilerelaunchcampaignmedium") }},
        {{ adapter.quote("mobilerelaunchcampaignsource") }},
        {{ adapter.quote("mobilerelaunchcampaignterm") }},
        {{ adapter.quote("mobilerelaunchcampaigntrackingcode") }},
        {{ adapter.quote("mobilerelaunchcampaigntrackingcode_name") }},
        {{ adapter.quote("mobileresolution") }},
        {{ adapter.quote("mobileupgrades") }}, #}
        {{ adapter.quote("monthly_visitor") }} AS MONTHLY_VISITOR, --masthead
        {# {{ adapter.quote("mvvar1") }},
        {{ adapter.quote("mvvar1_instances") }},
        {{ adapter.quote("mvvar2") }},
        {{ adapter.quote("mvvar2_instances") }},
        {{ adapter.quote("mvvar3") }},
        {{ adapter.quote("mvvar3_instances") }},
        {{ adapter.quote("namespace") }}, #}
        {{ adapter.quote("new_visit") }} AS NEW_VISIT, --masthead
        {{ adapter.quote("os") }} AS OS, --replatform
        {# {{ adapter.quote("p_plugins") }},
        {{ adapter.quote("page_event") }},
        {{ adapter.quote("page_event_var1") }},
        {{ adapter.quote("page_event_var2") }},
        {{ adapter.quote("page_event_var3") }},
        {{ adapter.quote("page_type") }}, #}
        {{ adapter.quote("page_url") }} AS PAGE_URL, -- replatfrom
        {{ adapter.quote("pagename") }} AS PAGENAME, -- replatfrom
        {# {{ adapter.quote("paid_search") }},
        {{ adapter.quote("partner_plugins") }},
        {{ adapter.quote("persistent_cookie") }},
        {{ adapter.quote("plugins") }},
        {{ adapter.quote("pointofinterest") }},
        {{ adapter.quote("pointofinterestdistance") }},
        {{ adapter.quote("post_adclassificationcreative") }},
        {{ adapter.quote("post_adload") }},
        {{ adapter.quote("post_amo_ef_id") }},
        {{ adapter.quote("post_browser_height") }},
        {{ adapter.quote("post_browser_width") }},
        {{ adapter.quote("post_campaign") }},
        {{ adapter.quote("post_channel") }},
        {{ adapter.quote("post_clickmaplink") }},
        {{ adapter.quote("post_clickmaplinkbyregion") }},
        {{ adapter.quote("post_clickmappage") }},
        {{ adapter.quote("post_clickmapregion") }},
        {{ adapter.quote("post_cookies") }},
        {{ adapter.quote("post_currency") }}, #}
        {{ adapter.quote("post_cust_hit_time_gmt") }} AS POST_CUST_HIT_TIME_GMT, --masthead
        {{ adapter.quote("post_cust_visid") }} AS POST_CUST_VISID, --masthead
        {{ adapter.quote("post_ef_id") }} AS POST_EF_ID,  --masthead
        {{ adapter.quote("post_evar1") }} AS POST_EVAR1,  --masthead & replatform
        {{ adapter.quote("post_evar2") }} AS POST_EVAR2,   --masthead & replatform
        {{ adapter.quote("post_evar3") }} AS POST_EVAR3,   --masthead & replatform
        {{ adapter.quote("post_evar4") }} AS POST_EVAR4,   --masthead & replatform
        {{ adapter.quote("post_evar5") }} AS POST_EVAR5,   --replatform
        {{ adapter.quote("post_evar6") }} AS POST_EVAR6,   --replatform
        {{ adapter.quote("post_evar7") }} AS POST_EVAR7,   --replatform
        {{ adapter.quote("post_evar8") }} AS POST_EVAR8,   --replatform
        {{ adapter.quote("post_evar9") }} AS POST_EVAR9,   --replatform
        {{ adapter.quote("post_evar10") }} AS POST_EVAR10,   --replatform
        {{ adapter.quote("post_evar11") }} AS POST_EVAR11,-- masthead & replatform
        {{adapter.quote("post_evar12") }} AS POST_EVAR12, --replatfrom
        {{ adapter.quote("post_evar13") }} AS POST_EVAR13, --replatform
        {{ adapter.quote("post_evar14") }} AS POST_EVAR14, --masthead & replatform
        {{ adapter.quote("post_evar15") }} AS POST_EVAR15, --replatform
        {{ adapter.quote("post_evar16") }} AS POST_EVAR16, --replatform
        {{ adapter.quote("post_evar17") }} AS POST_EVAR17, --replatform
        {{ adapter.quote("post_evar18") }} AS POST_EVAR18, --replatform
        {{ adapter.quote("post_evar19") }} AS POST_EVAR19, --replatform
        {{ adapter.quote("post_evar20") }} AS POST_EVAR20, --replatform
        {{ adapter.quote("post_evar21") }} AS POST_EVAR21,
        {#
        {{ adapter.quote("post_evar22") }}, #}
        {{ adapter.quote("post_evar23") }} AS POST_EVAR23, --masthead
        {# {{ adapter.quote("post_evar24") }} AS POST_EVAR24, #}
        {{ adapter.quote("post_evar25") }}  AS POST_EVAR25,  --masthead
        {# {{ adapter.quote("post_evar26") }},
        {{ adapter.quote("post_evar27") }},  #}
        {{ adapter.quote("post_evar28") }} AS POST_EVAR28,   --masthead
        {{ adapter.quote("post_evar29") }} AS POST_EVAR29,
        {#
        {{ adapter.quote("post_evar30") }},  #}
        {{ adapter.quote("post_evar31") }} AS POST_EVAR31,   --masthead
        {# {{ adapter.quote("post_evar32") }},
        {{ adapter.quote("post_evar33") }},
        {{ adapter.quote("post_evar34") }},
        {{ adapter.quote("post_evar35") }},
        {{ adapter.quote("post_evar36") }},
        {{ adapter.quote("post_evar37") }},
        {{ adapter.quote("post_evar38") }},
        {{ adapter.quote("post_evar39") }},
        {{ adapter.quote("post_evar40") }},
        {{ adapter.quote("post_evar41") }},
        {{ adapter.quote("post_evar42") }},
        {{ adapter.quote("post_evar43") }},
        {{ adapter.quote("post_evar44") }},
        {{ adapter.quote("post_evar45") }},
        {{ adapter.quote("post_evar46") }},
        {{ adapter.quote("post_evar47") }},
        {{ adapter.quote("post_evar48") }},
        {{ adapter.quote("post_evar49") }},
        {{ adapter.quote("post_evar50") }},
        {{ adapter.quote("post_evar51") }},
        {{ adapter.quote("post_evar52") }},
        {{ adapter.quote("post_evar53") }},
        {{ adapter.quote("post_evar54") }},
        {{ adapter.quote("post_evar55") }},
        {{ adapter.quote("post_evar56") }},
        {{ adapter.quote("post_evar57") }},
        {{ adapter.quote("post_evar58") }},
        {{ adapter.quote("post_evar59") }},
        {{ adapter.quote("post_evar60") }},
        {{ adapter.quote("post_evar61") }},
        {{ adapter.quote("post_evar62") }},
        {{ adapter.quote("post_evar63") }},
        {{ adapter.quote("post_evar64") }},
        {{ adapter.quote("post_evar65") }},
        {{ adapter.quote("post_evar66") }},
        {{ adapter.quote("post_evar67") }},
        {{ adapter.quote("post_evar68") }},
        {{ adapter.quote("post_evar69") }},
        {{ adapter.quote("post_evar70") }},
        {{ adapter.quote("post_evar71") }},
        {{ adapter.quote("post_evar72") }},  #}
        {{ adapter.quote("post_evar73") }} AS POST_EVAR73, --masthead
        {# {{ adapter.quote("post_evar74") }},
        {{ adapter.quote("post_evar75") }},
        {{ adapter.quote("post_evar76") }},
        {{ adapter.quote("post_evar77") }},
        {{ adapter.quote("post_evar78") }},
        {{ adapter.quote("post_evar79") }},
        {{ adapter.quote("post_evar80") }},
        {{ adapter.quote("post_evar81") }},
        {{ adapter.quote("post_evar82") }},
        {{ adapter.quote("post_evar83") }},  #}
        {{ adapter.quote("post_evar84") }} AS POST_EVAR84, --masthead
        {# {{ adapter.quote("post_evar85") }},
        {{ adapter.quote("post_evar86") }},
        {{ adapter.quote("post_evar87") }},
        {{ adapter.quote("post_evar88") }},
        {{ adapter.quote("post_evar89") }},
        {{ adapter.quote("post_evar90") }}, #}
        {{ adapter.quote("post_evar91") }} AS POST_EVAR91, --masthead
        {# {{ adapter.quote("post_evar92") }},
        {{ adapter.quote("post_evar93") }},
        {{ adapter.quote("post_evar94") }}, #}
        {{ adapter.quote("post_evar95") }}  AS POST_EVAR95, --masthead
        {{ adapter.quote("post_evar96") }} AS POST_EVAR96,  --masthead
        {# {{ adapter.quote("post_evar97") }},
        {{ adapter.quote("post_evar98") }},
        {{ adapter.quote("post_evar99") }},
        {{ adapter.quote("post_evar100") }},
        {{ adapter.quote("post_evar101") }},
        {{ adapter.quote("post_evar102") }},
        {{ adapter.quote("post_evar103") }},
        {{ adapter.quote("post_evar104") }},
        {{ adapter.quote("post_evar105") }}, #}
        {{ adapter.quote("post_evar106") }} AS POST_EVAR106,  --masthead
        {# {{ adapter.quote("post_evar107") }},
        {{ adapter.quote("post_evar108") }},
        {{ adapter.quote("post_evar109") }},
        {{ adapter.quote("post_evar110") }},
        {{ adapter.quote("post_evar111") }},
        {{ adapter.quote("post_evar112") }},
        {{ adapter.quote("post_evar113") }},
        {{ adapter.quote("post_evar114") }},
        {{ adapter.quote("post_evar115") }},
        {{ adapter.quote("post_evar116") }},
        {{ adapter.quote("post_evar117") }},
        {{ adapter.quote("post_evar118") }},
        {{ adapter.quote("post_evar119") }},
        {{ adapter.quote("post_evar120") }},
        {{ adapter.quote("post_evar121") }},
        {{ adapter.quote("post_evar122") }},
        {{ adapter.quote("post_evar123") }},
        {{ adapter.quote("post_evar124") }},
        {{ adapter.quote("post_evar125") }},
        {{ adapter.quote("post_evar126") }},
        {{ adapter.quote("post_evar127") }},
        {{ adapter.quote("post_evar128") }},
        {{ adapter.quote("post_evar129") }},
        {{ adapter.quote("post_evar130") }},
        {{ adapter.quote("post_evar131") }},
        {{ adapter.quote("post_evar132") }},
        {{ adapter.quote("post_evar133") }},
        {{ adapter.quote("post_evar134") }},
        {{ adapter.quote("post_evar135") }},
        {{ adapter.quote("post_evar136") }},
        {{ adapter.quote("post_evar137") }},
        {{ adapter.quote("post_evar138") }},
        {{ adapter.quote("post_evar139") }},
        {{ adapter.quote("post_evar140") }},
        {{ adapter.quote("post_evar141") }},
        {{ adapter.quote("post_evar142") }}, #}
        {{ adapter.quote("post_evar143") }}  AS POST_EVAR143,--masthead
        {# {{ adapter.quote("post_evar144") }},
        {{ adapter.quote("post_evar145") }},
        {{ adapter.quote("post_evar146") }},
        {{ adapter.quote("post_evar147") }},
        {{ adapter.quote("post_evar148") }},
        {{ adapter.quote("post_evar149") }},
        {{ adapter.quote("post_evar150") }},
        {{ adapter.quote("post_evar151") }},
        {{ adapter.quote("post_evar152") }},
        {{ adapter.quote("post_evar153") }},
        {{ adapter.quote("post_evar154") }},
        {{ adapter.quote("post_evar155") }},
        {{ adapter.quote("post_evar156") }},
        {{ adapter.quote("post_evar157") }},
        {{ adapter.quote("post_evar158") }},
        {{ adapter.quote("post_evar159") }},
        {{ adapter.quote("post_evar160") }},
        {{ adapter.quote("post_evar161") }},
        {{ adapter.quote("post_evar162") }}, #}
        {{ adapter.quote("post_evar163") }}  AS POST_EVAR163,  ----masthead
        {# {{ adapter.quote("post_evar164") }},
        {{ adapter.quote("post_evar165") }},
        {{ adapter.quote("post_evar166") }},
        {{ adapter.quote("post_evar167") }},
        {{ adapter.quote("post_evar168") }},
        {{ adapter.quote("post_evar169") }},
        {{ adapter.quote("post_evar170") }},
        {{ adapter.quote("post_evar171") }},
        {{ adapter.quote("post_evar172") }},
        {{ adapter.quote("post_evar173") }},
        {{ adapter.quote("post_evar174") }},
        {{ adapter.quote("post_evar175") }},
        {{ adapter.quote("post_evar176") }},
        {{ adapter.quote("post_evar177") }},
        {{ adapter.quote("post_evar178") }},
        {{ adapter.quote("post_evar179") }},
        {{ adapter.quote("post_evar180") }},
        {{ adapter.quote("post_evar181") }},
        {{ adapter.quote("post_evar182") }},
        {{ adapter.quote("post_evar183") }},
        {{ adapter.quote("post_evar184") }},
        {{ adapter.quote("post_evar185") }},
        {{ adapter.quote("post_evar186") }},
        {{ adapter.quote("post_evar187") }},
        {{ adapter.quote("post_evar188") }},
        {{ adapter.quote("post_evar189") }},
        {{ adapter.quote("post_evar190") }},
        {{ adapter.quote("post_evar191") }},
        {{ adapter.quote("post_evar192") }},
        {{ adapter.quote("post_evar193") }},
        {{ adapter.quote("post_evar194") }},
        {{ adapter.quote("post_evar195") }},
        {{ adapter.quote("post_evar196") }},
        {{ adapter.quote("post_evar197") }},
        {{ adapter.quote("post_evar198") }},
        {{ adapter.quote("post_evar199") }},
        {{ adapter.quote("post_evar200") }},
        {{ adapter.quote("post_evar201") }},
        {{ adapter.quote("post_evar202") }},
        {{ adapter.quote("post_evar203") }},
        {{ adapter.quote("post_evar204") }},
        {{ adapter.quote("post_evar205") }},
        {{ adapter.quote("post_evar206") }},
        {{ adapter.quote("post_evar207") }},
        {{ adapter.quote("post_evar208") }},
        {{ adapter.quote("post_evar209") }},
        {{ adapter.quote("post_evar210") }},
        {{ adapter.quote("post_evar211") }},
        {{ adapter.quote("post_evar212") }},
        {{ adapter.quote("post_evar213") }},
        {{ adapter.quote("post_evar214") }},
        {{ adapter.quote("post_evar215") }},
        {{ adapter.quote("post_evar216") }},
        {{ adapter.quote("post_evar217") }},
        {{ adapter.quote("post_evar218") }},
        {{ adapter.quote("post_evar219") }},
        {{ adapter.quote("post_evar220") }},
        {{ adapter.quote("post_evar221") }},
        {{ adapter.quote("post_evar222") }},
        {{ adapter.quote("post_evar223") }},
        {{ adapter.quote("post_evar224") }},
        {{ adapter.quote("post_evar225") }},
        {{ adapter.quote("post_evar226") }},
        {{ adapter.quote("post_evar227") }},
        {{ adapter.quote("post_evar228") }},
        {{ adapter.quote("post_evar229") }},
        {{ adapter.quote("post_evar230") }},
        {{ adapter.quote("post_evar231") }},
        {{ adapter.quote("post_evar232") }},
        {{ adapter.quote("post_evar233") }},
        {{ adapter.quote("post_evar234") }},
        {{ adapter.quote("post_evar235") }},
        {{ adapter.quote("post_evar236") }},
        {{ adapter.quote("post_evar237") }},
        {{ adapter.quote("post_evar238") }},
        {{ adapter.quote("post_evar239") }},
        {{ adapter.quote("post_evar240") }},
        {{ adapter.quote("post_evar241") }},
        {{ adapter.quote("post_evar242") }},
        {{ adapter.quote("post_evar243") }},
        {{ adapter.quote("post_evar244") }},
        {{ adapter.quote("post_evar245") }},
        {{ adapter.quote("post_evar246") }},
        {{ adapter.quote("post_evar247") }},
        {{ adapter.quote("post_evar248") }},
        {{ adapter.quote("post_evar249") }},
        {{ adapter.quote("post_evar250") }}, #}
        {{ adapter.quote("post_event_list") }} AS POST_EVENT_LIST, --masthead
        {# {{ adapter.quote("post_hier1") }},
        {{ adapter.quote("post_hier2") }},
        {{ adapter.quote("post_hier3") }},
        {{ adapter.quote("post_hier4") }},
        {{ adapter.quote("post_hier5") }},
        {{ adapter.quote("post_java_enabled") }},
        {{ adapter.quote("post_keywords") }},
        {{ adapter.quote("post_mc_audiences") }},
        {{ adapter.quote("post_mobileaction") }},
        {{ adapter.quote("post_mobileappid") }},
        {{ adapter.quote("post_mobilecampaigncontent") }},
        {{ adapter.quote("post_mobilecampaignmedium") }},
        {{ adapter.quote("post_mobilecampaignname") }},
        {{ adapter.quote("post_mobilecampaignsource") }},
        {{ adapter.quote("post_mobilecampaignterm") }}, #}
        {{ adapter.quote("post_mobiledayofweek") }} AS POST_MOBILE_DAY_OF_WEEK, --replatform
        {# {{ adapter.quote("post_mobiledayssincefirstuse") }}, #}
        {{ adapter.quote("post_mobiledayssincelastuse") }} AS POST_MOBILE_DAYS_SINCE_LAST_USE, --repltform
        {{ adapter.quote("post_mobiledevice") }} as POST_MOBILEDEVICE, --repltform
        {{ adapter.quote("post_mobilehourofday") }} as POST_MOBILE_HOUR_OF_DAY, --repltform
        {# {{ adapter.quote("post_mobileinstalldate") }},
        {{ adapter.quote("post_mobilelaunchnumber") }},
        {{ adapter.quote("post_mobileltv") }},
        {{ adapter.quote("post_mobilemessagebuttonname") }},
        {{ adapter.quote("post_mobilemessageclicks") }},
        {{ adapter.quote("post_mobilemessageid") }},
        {{ adapter.quote("post_mobilemessageid_dest") }},
        {{ adapter.quote("post_mobilemessageid_name") }},
        {{ adapter.quote("post_mobilemessageid_type") }},
        {{ adapter.quote("post_mobilemessageimpressions") }},
        {{ adapter.quote("post_mobilemessageonline") }},
        {{ adapter.quote("post_mobilemessagepushoptin") }},
        {{ adapter.quote("post_mobilemessagepushpayloadid") }},
        {{ adapter.quote("post_mobilemessagepushpayloadid_name") }},
        {{ adapter.quote("post_mobilemessageviews") }},
        {{ adapter.quote("post_mobileosversion") }},
        {{ adapter.quote("post_mobilepushoptin") }},
        {{ adapter.quote("post_mobilepushpayloadid") }},
        {{ adapter.quote("post_mobileresolution") }},
        {{ adapter.quote("post_mvvar1") }},
        {{ adapter.quote("post_mvvar1_instances") }},
        {{ adapter.quote("post_mvvar2") }},
        {{ adapter.quote("post_mvvar2_instances") }},
        {{ adapter.quote("post_mvvar3") }},
        {{ adapter.quote("post_mvvar3_instances") }}, #}
        {{ adapter.quote("post_page_event") }}  AS POST_PAGE_EVENT,---masthead
        {# {{ adapter.quote("post_page_event_var1") }},
        {{ adapter.quote("post_page_event_var2") }},
        {{ adapter.quote("post_page_event_var3") }},
        {{ adapter.quote("post_page_type") }}, #}
        {{ adapter.quote("post_page_url") }} AS POST_PAGE_URL,
        {{ adapter.quote("post_pagename") }} AS POST_PAGENAME, ---masthead
        {{ adapter.quote("post_pagename_no_url") }}  AS POST_PAGENAME_NO_URL,--masthead
        {# {{ adapter.quote("post_partner_plugins") }},
        {{ adapter.quote("post_persistent_cookie") }},
        {{ adapter.quote("post_pointofinterest") }},
        {{ adapter.quote("post_pointofinterestdistance") }}, #}
        {{ adapter.quote("post_product_list") }} AS  POST_PRODUCT_LIST,  ---masthead
        {{ adapter.quote("post_prop1") }} AS POST_PROP1,  ----masthead
        {{ adapter.quote("post_prop2") }} AS POST_PROP2,   --masthead
        {{ adapter.quote("post_prop3") }} AS POST_PROP3,  --masthead & replatform
        {{ adapter.quote("post_prop4") }} AS POST_PROP4,  --masthead &replatform
        {{ adapter.quote("post_prop5") }} AS POST_PROP5,  --replatform
        {# {{ adapter.quote("post_prop6") }},
        {{ adapter.quote("post_prop7") }}, #}
        {{ adapter.quote("post_prop8") }} AS POST_PROP8,   ----masthead
        {{ adapter.quote("post_prop9") }} AS POST_PROP9,   ----replatform
        {{ adapter.quote("post_prop10") }} AS POST_PROP10,   ----replatform
        {{ adapter.quote("post_prop11") }} AS POST_PROP11,   ----masthead & replatform
        {# {{ adapter.quote("post_prop12") }},
        {{ adapter.quote("post_prop13") }}, #}
        {{ adapter.quote("post_prop14") }} AS POST_PROP14,  --masthead & replatform
        {# {{ adapter.quote("post_prop15") }},
        {{ adapter.quote("post_prop16") }},
        {{ adapter.quote("post_prop17") }},
        {{ adapter.quote("post_prop18") }},
        {{ adapter.quote("post_prop19") }},
        {{ adapter.quote("post_prop20") }},
        {{ adapter.quote("post_prop21") }},
        #}
        {{ adapter.quote("post_prop22") }} AS POST_PROP22,
        {#
        {{ adapter.quote("post_prop23") }},
        {{ adapter.quote("post_prop24") }}, #}
        {{ adapter.quote("post_prop25") }} AS POST_PROP25,  --masthead
        {# {{ adapter.quote("post_prop26") }},
        {{ adapter.quote("post_prop27") }}, #}
        {{ adapter.quote("post_prop28") }} AS POST_PROP28, --masthead
        {{ adapter.quote("post_prop29") }} ,
        {#
        {{ adapter.quote("post_prop30") }}, #}
        {{ adapter.quote("post_prop31") }} AS POST_PROP31, --masthead
        {# {{ adapter.quote("post_prop32") }},
        {{ adapter.quote("post_prop33") }},
        {{ adapter.quote("post_prop34") }},
        #}
        {{ adapter.quote("post_prop35") }} AS POST_PROP35,
        {#
        {{ adapter.quote("post_prop36") }},
        {{ adapter.quote("post_prop37") }},
        {{ adapter.quote("post_prop38") }},
        #}
        {{ adapter.quote("post_prop39") }} AS POST_PROP39,
        {#
        {{ adapter.quote("post_prop40") }},
        {{ adapter.quote("post_prop41") }},
        {{ adapter.quote("post_prop42") }},
        {{ adapter.quote("post_prop43") }},
        {{ adapter.quote("post_prop44") }},
        {{ adapter.quote("post_prop45") }},
        {{ adapter.quote("post_prop46") }}, #}
        {{ adapter.quote("post_prop47") }} AS POST_PROP47, --masthead
        {# {{ adapter.quote("post_prop48") }},
        {{ adapter.quote("post_prop49") }},
        {{ adapter.quote("post_prop50") }},
        {{ adapter.quote("post_prop51") }},
        {{ adapter.quote("post_prop52") }},
        {{ adapter.quote("post_prop53") }}, #}
        {{ adapter.quote("post_prop54") }} AS POST_PROP54,   --masthead
        {{ adapter.quote("post_prop55") }} AS POST_PROP55,   ----masthead
        {{ adapter.quote("post_prop56") }} AS POST_PROP56,   ----masthead
        {# {{ adapter.quote("post_prop57") }},
        {{ adapter.quote("post_prop58") }},
        {{ adapter.quote("post_prop59") }},
        {{ adapter.quote("post_prop60") }}, #}
        {{ adapter.quote("post_prop61") }}  AS POST_PROP61, --masthead
        {# {{ adapter.quote("post_prop62") }},
        {{ adapter.quote("post_prop63") }}, #}
        {{ adapter.quote("post_prop64") }} AS POST_PROP64, --masthead
        {# {{ adapter.quote("post_prop65") }},
        {{ adapter.quote("post_prop66") }},
        {{ adapter.quote("post_prop67") }},
        {{ adapter.quote("post_prop68") }},
        {{ adapter.quote("post_prop69") }},
        {{ adapter.quote("post_prop70") }},
        {{ adapter.quote("post_prop71") }},
        {{ adapter.quote("post_prop72") }},
        {{ adapter.quote("post_prop73") }},
        {{ adapter.quote("post_prop74") }},
        {{ adapter.quote("post_prop75") }},
        {{ adapter.quote("post_purchaseid") }},
        {{ adapter.quote("post_referrer") }},
        {{ adapter.quote("post_s_kwcid") }},
        {{ adapter.quote("post_search_engine") }},
        {{ adapter.quote("post_socialaccountandappids") }},
        {{ adapter.quote("post_socialassettrackingcode") }},
        {{ adapter.quote("post_socialauthor") }},
        {{ adapter.quote("post_socialaveragesentiment") }},
        {{ adapter.quote("post_socialaveragesentiment__deprecated_") }},
        {{ adapter.quote("post_socialcontentprovider") }},
        {{ adapter.quote("post_socialfbstories") }},
        {{ adapter.quote("post_socialfbstorytellers") }},
        {{ adapter.quote("post_socialinteractioncount") }},
        {{ adapter.quote("post_socialinteractiontype") }},
        {{ adapter.quote("post_sociallanguage") }},
        {{ adapter.quote("post_sociallatlong") }},
        {{ adapter.quote("post_sociallikeadds") }},
        {{ adapter.quote("post_sociallink") }},
        {{ adapter.quote("post_sociallink__deprecated_") }},
        {{ adapter.quote("post_socialmentions") }},
        {{ adapter.quote("post_socialowneddefinitioninsighttype") }},
        {{ adapter.quote("post_socialowneddefinitioninsightvalue") }},
        {{ adapter.quote("post_socialowneddefinitionmetric") }},
        {{ adapter.quote("post_socialowneddefinitionpropertyvspost") }},
        {{ adapter.quote("post_socialownedpostids") }},
        {{ adapter.quote("post_socialownedpropertyid") }},
        {{ adapter.quote("post_socialownedpropertyname") }},
        {{ adapter.quote("post_socialownedpropertypropertyvsapp") }},
        {{ adapter.quote("post_socialpageviews") }},
        {{ adapter.quote("post_socialpostviews") }},
        {{ adapter.quote("post_socialproperty") }},
        {{ adapter.quote("post_socialproperty__deprecated_") }},
        {{ adapter.quote("post_socialpubcomments") }},
        {{ adapter.quote("post_socialpubposts") }},
        {{ adapter.quote("post_socialpubrecommends") }},
        {{ adapter.quote("post_socialpubsubscribers") }},
        {{ adapter.quote("post_socialterm") }},
        {{ adapter.quote("post_socialtermslist") }},
        {{ adapter.quote("post_socialtermslist__deprecated_") }},
        {{ adapter.quote("post_socialtotalsentiment") }},
        {{ adapter.quote("post_state") }},
        {{ adapter.quote("post_survey") }},
        {{ adapter.quote("post_t_time_info") }},
        {{ adapter.quote("post_tnt") }},
        {{ adapter.quote("post_tnt_action") }},
        {{ adapter.quote("post_transactionid") }}, #}
        {{ adapter.quote("post_user_server") }} AS POST_USER_SERVER,  --masthead & replatfrom
        {# {{ adapter.quote("post_video") }},
        {{ adapter.quote("post_videoad") }},
        {{ adapter.quote("post_videoadinpod") }},
        {{ adapter.quote("post_videoadlength") }},
        {{ adapter.quote("post_videoadname") }},
        {{ adapter.quote("post_videoadplayername") }},
        {{ adapter.quote("post_videoadpod") }},
        {{ adapter.quote("post_videoadvertiser") }},
        {{ adapter.quote("post_videoauthorized") }},
        {{ adapter.quote("post_videocampaign") }},
        {{ adapter.quote("post_videochannel") }},
        {{ adapter.quote("post_videochapter") }},
        {{ adapter.quote("post_videocontenttype") }},
        {{ adapter.quote("post_videodaypart") }},
        {{ adapter.quote("post_videoepisode") }},
        {{ adapter.quote("post_videofeedtype") }},
        {{ adapter.quote("post_videogenre") }},
        {{ adapter.quote("post_videolength") }},
        {{ adapter.quote("post_videomvpd") }},
        {{ adapter.quote("post_videoname") }},
        {{ adapter.quote("post_videonetwork") }},
        {{ adapter.quote("post_videopath") }},
        {{ adapter.quote("post_videoplayername") }},
        {{ adapter.quote("post_videoqoebitrateaverageevar") }},
        {{ adapter.quote("post_videoqoebitratechangecountevar") }},
        {{ adapter.quote("post_videoqoebuffercountevar") }},
        {{ adapter.quote("post_videoqoebuffertimeevar") }},
        {{ adapter.quote("post_videoqoedroppedframecountevar") }},
        {{ adapter.quote("post_videoqoeerrorcountevar") }},
        {{ adapter.quote("post_videoqoeplayersdkerrors") }},
        {{ adapter.quote("post_videoqoetimetostartevar") }},
        {{ adapter.quote("post_videoseason") }},
        {{ adapter.quote("post_videosegment") }},
        {{ adapter.quote("post_videoshow") }},
        {{ adapter.quote("post_videoshowtype") }}, #}
        {{ adapter.quote("post_visid_high") }} AS POST_VISID_HIGH,  --masthead
        {{ adapter.quote("post_visid_low") }}  AS POST_VISID_LOW,  --masthead
        {{ adapter.quote("post_visid_type") }} AS POST_VISID_TYPE,  --masthead
        {# {{ adapter.quote("post_zip") }}, #}
        {{ adapter.quote("prev_page") }} AS PREV_PAGE,
        {#
        {{ adapter.quote("product_list") }},
        {{ adapter.quote("product_merchandising") }}, #}
        {{ adapter.quote("prop1") }} AS PROP1,   --masthead
        {{ adapter.quote("prop2") }} AS PROP2,   --masthead
        {{ adapter.quote("prop3") }}  AS PROP3,  ---masthead & repltform
        {{ adapter.quote("prop4") }} AS PROP4,   --masthead & replatform
        {{ adapter.quote("prop5") }} AS PROP5,   --replatform
        {# {{ adapter.quote("prop6") }},
        {{ adapter.quote("prop7") }}, #}
        {{ adapter.quote("prop8") }} AS PROP8, --replatform
        {{ adapter.quote("prop9") }} AS PROP9, --replatform
        {{ adapter.quote("prop10") }} AS PROP10, --repltform
        {{ adapter.quote("prop11") }} AS PROP11, --masthead & replatform
        {# {{ adapter.quote("prop12") }},
        {{ adapter.quote("prop13") }}, #}
        {{ adapter.quote("prop14") }} AS PROP14, --masthead & replatform
        {# {{ adapter.quote("prop15") }},
        {{ adapter.quote("prop16") }},
        {{ adapter.quote("prop17") }},
        {{ adapter.quote("prop18") }},
        {{ adapter.quote("prop19") }},
        {{ adapter.quote("prop20") }},
        {{ adapter.quote("prop21") }},
        {{ adapter.quote("prop22") }},
        {{ adapter.quote("prop23") }},
        {{ adapter.quote("prop24") }}, #}
        {{ adapter.quote("prop25") }}  AS PROP25, --masthead
        {# {{ adapter.quote("prop26") }},
        {{ adapter.quote("prop27") }}, #}
        {{ adapter.quote("prop28") }} AS PROP28, --masthead
        {# {{ adapter.quote("prop29") }},
        {{ adapter.quote("prop30") }}, #}
        {{ adapter.quote("prop31") }} AS PROP31, --masthead
        {# {{ adapter.quote("prop32") }},
        {{ adapter.quote("prop33") }},
        {{ adapter.quote("prop34") }},
        {{ adapter.quote("prop35") }},
        {{ adapter.quote("prop36") }},
        {{ adapter.quote("prop37") }},
        {{ adapter.quote("prop38") }},
        {{ adapter.quote("prop39") }},
        {{ adapter.quote("prop40") }},
        {{ adapter.quote("prop41") }},
        {{ adapter.quote("prop42") }},
        {{ adapter.quote("prop43") }},
        {{ adapter.quote("prop44") }},
        {{ adapter.quote("prop45") }},
        {{ adapter.quote("prop46") }}, #}
        {{ adapter.quote("prop47") }} AS PROP47, --masthead
        {# {{ adapter.quote("prop48") }},
        {{ adapter.quote("prop49") }},
        {{ adapter.quote("prop50") }},
        {{ adapter.quote("prop51") }},
        {{ adapter.quote("prop52") }},
        {{ adapter.quote("prop53") }}, #}
        {{ adapter.quote("prop54") }} AS PROP54, ----masthead
        {# {{ adapter.quote("prop55") }}, #}
        {{ adapter.quote("prop56") }} AS PROP56, --masthead
        {# {{ adapter.quote("prop57") }},
        {{ adapter.quote("prop58") }},
        {{ adapter.quote("prop59") }},
        {{ adapter.quote("prop60") }}, #}
        {{ adapter.quote("prop61") }} AS PROP61, --masthead
        {# {{ adapter.quote("prop62") }},
        {{ adapter.quote("prop63") }}, #}
        {{ adapter.quote("prop64") }} AS PROP64, --masthead
        {# {{ adapter.quote("prop65") }},
        {{ adapter.quote("prop66") }},
        {{ adapter.quote("prop67") }},
        {{ adapter.quote("prop68") }},
        {{ adapter.quote("prop69") }},
        {{ adapter.quote("prop70") }},
        {{ adapter.quote("prop71") }},
        {{ adapter.quote("prop72") }},
        {{ adapter.quote("prop73") }},
        {{ adapter.quote("prop74") }},
        {{ adapter.quote("prop75") }}, #}
        {{ adapter.quote("purchaseid") }} AS PURCHASE_ID, --masthead
        {{ adapter.quote("quarterly_visitor") }} AS QUARTERLY_VISITOR, --masthead & replatform
        {{ adapter.quote("ref_domain") }} AS REF_DOMAIN, --masthead
        {{ adapter.quote("ref_type") }} AS REF_TYPE,  --masthead
        {{ adapter.quote("referrer") }} AS REFERRER,  --masthead & replatform
        {# {{ adapter.quote("resocialpageviews") }},
        {{ adapter.quote("resolution") }},
        {{ adapter.quote("s_kwcid") }},
        {{ adapter.quote("s_resolution") }},
        {{ adapter.quote("sampled_hit") }},
        {{ adapter.quote("search_engine") }},
        {{ adapter.quote("search_page_num") }},
        {{ adapter.quote("secondary_hit") }},
        {{ adapter.quote("service") }},
        {{ adapter.quote("socialaccountandappids") }},
        {{ adapter.quote("socialassettrackingcode") }},
        {{ adapter.quote("socialauthor") }},
        {{ adapter.quote("socialaveragesentiment") }},
        {{ adapter.quote("socialaveragesentiment__deprecated_") }},
        {{ adapter.quote("socialcontentprovider") }},
        {{ adapter.quote("socialfbstories") }},
        {{ adapter.quote("socialfbstorytellers") }},
        {{ adapter.quote("socialinteractioncount") }},
        {{ adapter.quote("socialinteractiontype") }},
        {{ adapter.quote("sociallanguage") }},
        {{ adapter.quote("sociallatlong") }},
        {{ adapter.quote("sociallikeadds") }},
        {{ adapter.quote("sociallink") }},
        {{ adapter.quote("sociallink__deprecated_") }},
        {{ adapter.quote("socialmentions") }},
        {{ adapter.quote("socialowneddefinitioninsighttype") }},
        {{ adapter.quote("socialowneddefinitioninsightvalue") }},
        {{ adapter.quote("socialowneddefinitionmetric") }},
        {{ adapter.quote("socialowneddefinitionpropertyvspost") }},
        {{ adapter.quote("socialownedpostids") }},
        {{ adapter.quote("socialownedpropertyid") }},
        {{ adapter.quote("socialownedpropertyname") }},
        {{ adapter.quote("socialownedpropertypropertyvsapp") }},
        {{ adapter.quote("socialpageviews") }},
        {{ adapter.quote("socialpostviews") }},
        {{ adapter.quote("socialproperty") }},
        {{ adapter.quote("socialproperty__deprecated_") }},
        {{ adapter.quote("socialpubcomments") }},
        {{ adapter.quote("socialpubposts") }},
        {{ adapter.quote("socialpubrecommends") }},
        {{ adapter.quote("socialpubsubscribers") }},
        {{ adapter.quote("socialterm") }},
        {{ adapter.quote("socialtermslist") }},
        {{ adapter.quote("socialtermslist__deprecated_") }},
        {{ adapter.quote("socialtotalsentiment") }},
        {{ adapter.quote("sourceid") }},
        {{ adapter.quote("ssocialpubsubscribers") }},
        {{ adapter.quote("state") }}, #}
        {{ adapter.quote("stats_server") }} AS STATS_SERVER, --masthead & replatfrom
        {# {{ adapter.quote("survey_instances") }},
        {{ adapter.quote("t_time_info") }},
        {{ adapter.quote("tnt") }},
        {{ adapter.quote("tnt_action") }},
        {{ adapter.quote("tnt_instances") }},
        {{ adapter.quote("tnt_post_vista") }},
        {{ adapter.quote("transactionid") }},
        {{ adapter.quote("truncated_hit") }},
        {{ adapter.quote("ua_color") }},
        {{ adapter.quote("ua_os") }},
        {{ adapter.quote("ua_pixels") }},
        {{ adapter.quote("user_agent") }},
        {{ adapter.quote("user_hash") }}, #}
        {{ adapter.quote("user_server") }} AS USER_SERVER, --masthead & replatfrom
        {# {{ adapter.quote("userid") }},
        {{ adapter.quote("username") }},
        {{ adapter.quote("va_closer_detail") }},
        {{ adapter.quote("va_closer_id") }},
        {{ adapter.quote("va_finder_detail") }},
        {{ adapter.quote("va_finder_id") }},
        {{ adapter.quote("va_instance_event") }},
        {{ adapter.quote("va_new_engagement") }},
        {{ adapter.quote("video") }},
        {{ adapter.quote("videoad") }},
        {{ adapter.quote("videoadinpod") }},
        {{ adapter.quote("videoadlength") }},
        {{ adapter.quote("videoadname") }},
        {{ adapter.quote("videoadplayername") }},
        {{ adapter.quote("videoadpod") }},
        {{ adapter.quote("videoadvertiser") }},
        {{ adapter.quote("videoaudioalbum") }},
        {{ adapter.quote("videoaudioartist") }},
        {{ adapter.quote("videoaudioauthor") }},
        {{ adapter.quote("videoaudiolabel") }},
        {{ adapter.quote("videoaudiopublisher") }},
        {{ adapter.quote("videoaudiostation") }},
        {{ adapter.quote("videoauthorized") }},
        {{ adapter.quote("videoaverageminuteaudience") }},
        {{ adapter.quote("videocampaign") }},
        {{ adapter.quote("videochannel") }},
        {{ adapter.quote("videochapter") }},
        {{ adapter.quote("videochaptercomplete") }},
        {{ adapter.quote("videochapterstart") }},
        {{ adapter.quote("videochaptertime") }},
        {{ adapter.quote("videocontenttype") }},
        {{ adapter.quote("videodaypart") }},
        {{ adapter.quote("videoepisode") }},
        {{ adapter.quote("videofeedtype") }},
        {{ adapter.quote("videogenre") }},
        {{ adapter.quote("videolength") }},
        {{ adapter.quote("videomvpd") }},
        {{ adapter.quote("videoname") }},
        {{ adapter.quote("videonetwork") }},
        {{ adapter.quote("videopath") }},
        {{ adapter.quote("videopause") }},
        {{ adapter.quote("videopausecount") }},
        {{ adapter.quote("videopausetime") }},
        {{ adapter.quote("videoplay") }},
        {{ adapter.quote("videoplayername") }},
        {{ adapter.quote("videoprogress10") }},
        {{ adapter.quote("videoprogress25") }},
        {{ adapter.quote("videoprogress50") }},
        {{ adapter.quote("videoprogress75") }},
        {{ adapter.quote("videoprogress96") }},
        {{ adapter.quote("videoqoebitrateaverage") }},
        {{ adapter.quote("videoqoebitrateaverageevar") }},
        {{ adapter.quote("videoqoebitratechange") }},
        {{ adapter.quote("videoqoebitratechangecountevar") }},
        {{ adapter.quote("videoqoebuffer") }},
        {{ adapter.quote("videoqoebuffercountevar") }},
        {{ adapter.quote("videoqoebuffertimeevar") }},
        {{ adapter.quote("videoqoedropbeforestart") }},
        {{ adapter.quote("videoqoedroppedframecountevar") }},
        {{ adapter.quote("videoqoedroppedframes") }},
        {{ adapter.quote("videoqoeerror") }},
        {{ adapter.quote("videoqoeerrorcountevar") }},
        {{ adapter.quote("videoqoeextneralerrors") }},
        {{ adapter.quote("videoqoeplayersdkerrors") }},
        {{ adapter.quote("videoqoetimetostartevar") }},
        {{ adapter.quote("videoresume") }},
        {{ adapter.quote("videoseason") }},
        {{ adapter.quote("videosegment") }},
        {{ adapter.quote("videoshow") }},
        {{ adapter.quote("videoshowtype") }},
        {{ adapter.quote("videostreamtype") }},
        {{ adapter.quote("videototaltime") }},
        {{ adapter.quote("videouniquetimeplayed") }}, #}
        {{ adapter.quote("visid_high") }} AS VISID_HIGH, --masthead
        {{ adapter.quote("visid_low") }} AS VISID_LOW, --masthead
        {{ adapter.quote("visid_new") }} AS VISID_NEW, --masthead
        {{ adapter.quote("visid_timestamp") }} AS VISID_TIMESTAMP,  --masthead
        {{ adapter.quote("visid_type") }} AS VISID_TYPE, --masthead
        {{ adapter.quote("visit_keywords") }} AS VISIT_KEYWORDS,
        {{ adapter.quote("visit_num") }} AS VISIT_NUM, --masthead & replatform
        {{ adapter.quote("visit_page_num") }} AS VISIT_PAGE_NUM, --masthead
        {{ adapter.quote("visit_ref_domain") }} AS VISIT_REF_DOMAIN, --masthead
        {{ adapter.quote("visit_ref_type") }} AS VISIT_REF_TYPE, --masthead
        {{ adapter.quote("visit_referrer") }} AS VISIT_REFERRER, --masthead
        {{ adapter.quote("visit_search_engine") }} AS VISIT_SEARCH_ENGINE, --masthead
        {{ adapter.quote("visit_start_page_url") }} AS VISIT_START_PAGE_URL, --masthead
        {{ adapter.quote("visit_start_pagename") }} AS VISIT_START_PAGENAME,--masthead
        {{ adapter.quote("visit_start_time_gmt") }} AS VISIT_START_TIME_GMT, --masthead
        {{ adapter.quote("weekly_visitor") }} AS WEEKLY_VISITOR, --masthead
        {{ adapter.quote("yearly_visitor") }} as YEARLY_VISITOR,
        {{ adapter.quote("zip") }} AS ZIP_CODE, --replatfrom

        _TABLE_SUFFIX AS TABLE_SUFFIX_VALUE,
        PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(_TABLE_SUFFIX, '0000')) AS TABLE_SUFFIX_TIME

    from source
    LEFT JOIN referrer_type r ON source.ref_type = r.id
    LEFT JOIN event_lookup e ON source.event_list = e.id
 )
 select * from renamed
