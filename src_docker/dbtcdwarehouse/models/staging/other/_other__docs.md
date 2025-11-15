dbo	donation
dbo	paf_alternative_suburb_name
dbo	paf_alternative_street_name
dbo	delta.inctest
dbo	complaint_type
dbo	complaints
dbo	qualtrics_surveyresponse_export_log
dbo	seq_test
dbo	paf_delivery_addres
dbo	paf_delivery_address
dbo	paf_alternative_town_city_name
dbo	paf_geo_dem_address

-- delta is cache in between staging & intermediate modelling (updated from alteryx)
-- this is temporary or discardable data used in experian data studio for customer whitewashing
delta	matrix_cancellations_price_increase_analysis
delta	all_active_subscriber_all
delta	masthead_hist <-- matrix? (intermediate)
delta	debtors_check
delta	prospect_contact_history
delta	Mag_Pending
delta	matrix_rounds
delta	scv_prospect_statistics

log	etl_job
raw	link_subscriber_retention
salesforce	account

# prod stage list for comparison
dbo	user
dbo	sysdiagrams
dbo	user_paf_address
dbo	paf_address
dbo	nbly_user_export20211119
delta	supporter_subs
delta	active_subscriber_all
delta	Mag_Pending
delta	matrix_subscription_delta
delta	matrix_subscriber_Mosaic_type_profiling
delta	MosaicTypeProfileStatistic
delta	matrix_subsID_phonelist
delta	idm_usr_test
delta	neighbourly_members_confirmed
delta	debtors_check
delta	matrix_rounds
delta	scv_unprocessed_delta
delta	scv_delta_discrepancy
delta	masthead_hist
delta	prospect_contact_history
delta	matrix_cancellations_price_increase_analysis
delta	scv_processed_delta
delta	MosaicTypeProfile
log	surveyresponse_export_log_bckup
log	etl_job
hexaevent	cust_detail_customer_id
hexaevent	cust_detail
