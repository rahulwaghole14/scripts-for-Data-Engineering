## Links
Api docs for user search [here](https://docs.piano.io/api?endpoint=post~2F~2Fpublisher~2Fuser~2Fsearch)
FAQ for user search [here](https://docs.piano.io/faq-article/how-to-search-for-users-who-updated-their-custom-fields-during-a-daterange-via-api/)

## Advanced search of app’s users

Searches the users of a given app basing on various criteria.
Based on search in an ElasticSearch replica of the prod DB. If ElasticSearch is not enabled for the app, the API endpoint queries the prod DB's user table directly, which might result in time out.
The "limit" of users per query should be within 1,000.
For quick search, use /publisher/user/list.

Clients using Piano ID can get an individual user by calling /id/api/v1/publisher/users/get (recommended).

METHOD `POST`
PATH `/publisher/user/search`

## Endpoint Response `User`

| Parameter | Description | Type |
| --- | --- | --- |
| `User` | The user's first name | string |
| `last_name` | The user's last name | string |
| `email` | The user's email address (single) | string |
| `personal_name` | The user's personal name. Name and surname ordered as per locale | string |
| `uid` | The user's ID | string |
| `image1` | The user's profile image | string |
| `create_date` | The user creation date | date-time |
| `reset_password_email_sent` | Whether a reset password email is sent | boolean |
| `custom_fields` | array | |
| `last_visit` | The date of the user's last visit | date-time |
| `last_login` | The last login stamp | date-time |


## Endpoint Parameters

| Parameter | Value | Description | Type |
| --- | --- | --- | --- |
| `aid` | (required) | The application ID | string |
| `uid` | | The user ID | string |
| `exclude_cf_metadata` | | Whether to exclude custom fields metadata | boolean |
| `name` | | Finds users whose names start with this keyword | string |
| `email` | | Finds users who contain this keyword in their emails | string |
| `registered_from` | | Find users which was registered from selected date | string |
| `registered_until` | | Find users which was registered until selected date | string |
| `access_to_resources` | | Find users who have access to select resources. Resource IDs (RIDs) are accepted values | array |
| `converted_terms` | | Find users who have converted on select terms. Term IDs are accepted values | array |
| `access_from` | | Find users who have any ACTIVE access from this date. The date format is a unix timestamp | string |
| `access_until` | | Find users who have any access until this date. The date format is a unix timestamp | string |
| `converted_term_from` | | Find users who have converted on any term from this date. The date format is a unix timestamp | string |
| `converted_term_until` | | Find users who have converted on any term until this date. The date format is a unix timestamp | string |
| `converted_term_sharing_type` | | Find users who have converted on any term and have specific sharing type | string |
| `redeemed_promotions` | | Find users who have redeemed select promotions. Promotion public IDs are accepted values. Promotion public IDs can be obtained by visiting Manage→Promotions from the Piano Dashboard | array |
| `redeemed_promotion_from` | | Find users who have redeemed on any promotion on or after this date. The date format is a unix timestamp | string |
| `redeemed_promotion_until` | | Find users who have redeemed on any promotion on or before this date. The date format is a unix timestamp | string |
| `trial_period_is_active` | | Find users who have any trial subscription at the present time | boolean |
| `has_trial_period` | | If user has ever used trial period | boolean |
| `has_access` | | Find users who have any type of access (access that is not expired or will never expire) | boolean |
| `has_conversion_term` | | Find users who have converted on any term | boolean |
| `has_redeemed_promotion` | | If user has ever used promo code | boolean |
| `include_trial_redemptions` | | Find users who redeemed a promotion, including those redeemed when signing up for a free trial. In these cases, the promotion had not been applied during the period of your search but were applied as soon as the trial period ended | boolean |
| `converted_term_types` | | Find users who have converted on particular types of terms. The accepted value of each type of term is a number: 0 (N/A), 1 (payment), 2 (ad view), 3 (registration), 4 (newsletter), 5 (external), 6 (custom), 7 (access granted), and 8 (gift). | array |
| `has_conversion_term_type` | | Find users which have conversion terms for selected term types | boolean |
| `spent_money_currency` | | Select the currency of the payments to take into account. Format is ISO 4217 (Ex: USD) | string |
| `spent_money_from` | | Find users who spent above a specified monetary value across all of their purchases and conversions. This value is formatted as a decimal. (Example: 10.03. to represent $10.03 or £10.03 or €10.03) | number |
| `spent_money_until` | | Find users who spent below a specified monetary value across all of their purchases and conversions. This value is formatted as a decimal. (Ex: 10.03. to represent $10.03 or £10.03 or €10.03) | number |
| `spent_from_date` | | Find users who bought something on or after this date. The date format is a unix timestamp | string |
| `spent_until_date` | | Find users who bought something on or before this date. The date format is a unix timestamp | string |
| `payment_methods` | | Find users who have used specific payment methods.The accepted values for each type of payment method: 1 (PayPal), 4 (BrainTree), 8 (AmazonMWS), 11 (PayPalBT), 12 (WorldPay_HPP), 13 (WorldPay_PayPal), 14 (WorldPay_Ideal), 15 (WorldPay_ELV), 16 (Spreedly_CC), 17 (Spreedly_Stripe_CC), 18 (Spreedly_Beanstream), 19 (EdgilPayway), 20 (WorldPay_CC_Token), 21 (Spreedly_PayU_Latam). | array |
| `billing_failure_from` | | Find users who had problems with auto-renewal of any subscription on or after this date. The date format is a unix timestamp | string |
| `billing_failure_until` | | Find users who had problems with auto-renewal of any subscription on or before this date. The date format is a unix timestamp | string |
| `had_billing_failure` | | Finds users who had any problems with billing | boolean |
| `has_payment` | | Finds users who have made any payment. Refunded payments are not taken into account. So if user had a payment and refunded it, he will not presented in the result list | boolean |
| `upi_ext_customer_id` | | Find users which have given external customer id | string |
| `credit_card_will_expire` | | Find users whose cards will expire in selected dates | string |
| `active_subscription_to_resources` | | Find users who have active subscriptions to specified resources. Resource IDs (RIDs) are accepted values | array |
| `has_active_subscription` | | Finds users who have any active subscription. Set field to true in order to get only active users | boolean |
| `subscription_start_from` | | Finds users who have any subscription starting on or after this date. This parameter depends on the has_subscription_starts parameter returning True. The date format is a unix timestamp | string |
| `subscription_start_until` | | Finds users who have any subscription that started on or before this date. This parameter depends on the has_subscription_starts parameter returning True. The date format is a unix timestamp | string |
| `subscription_renew_from` | | Finds users who have any subscription renewing on or after this date. This parameter depends on the has_subscription_will_renew parameter. The date format is a unix timestamp | string |
| `subscription_renew_until` | | Finds users who have any subscription renewing on or before this date. This parameter depends on the has_subscription_will_renew parameter. The date format is a unix timestamp | string |
| `subscription_expire_from` | | Finds users who have any subscription expiring on or after this date. This parameter depends on the has_subscription_will_expire parameter. The date format is a unix timestamp | string |
| `subscription_expire_until` | | Finds users who have any subscription expiring on or before this date. This parameter depends on the has_subscription_will_expire parameter. The date format is a unix timestamp | string |
| `trial_expire_from` | | Finds users who have any trial subscription expiring on or after this date. The date format is a unix timestamp | string |
| `trial_expire_until` | | Finds users who have any trial subscription expiring on or before this date. The date format is a unix timestamp | string |
| `has_any_subscriptions` | | Finds users with any subscriptions, including expired and canceled subscriptions | boolean |
| `has_subscription_will_renew` | | Finds users who have any subscription with enabled auto-renew | boolean |
| `has_subscription_will_expire` | | Finds users who have any subscription which will expire | boolean |
| `has_subscription_starts` | | Finds users which have started subscription | boolean |
| `has_unresolved_inquiry` | | Finds users who have any unresolved inquiry | boolean |
| `submitted_inquiry_from` | | Finds users who have any submitted inquiry on or after this date. The date format is a unix timestamp | string |
| `submitted_inquiry_until` | | Finds users who have any submitted inquiry on or before this date. The date format is a unix timestamp | string |
| `received_response_from` | | Finds users who received any inquiry response on or after this date. The date format is a unix timestamp | string |
| `received_response_until` | | Finds users who received any inquiry response on or before this date. The date format is a unix timestamp | string |
| `resolved_inquiry_from` | | Finds users who have any resolved inquiry on or after this date. The date format is a unix timestamp | string |
| `resolved_inquiry_until` | | Finds users who have any resolved inquiry on or before this date. The date format is a unix timestamp | string |
| `has_submitted_inquiry` | | Finds users with submitted inquiries | boolean |
| `has_received_response_inquiry` | | Finds users with any inquiries that have been responded to | boolean |
| `has_resolved_inquiry` | | Finds users with any resolved inquiry | boolean |
| `has_licensing_contract_redemptions` | | Finds users who redeemed any licensing contract | boolean |
| `selected_licensees` | | Finds users who redeemed licensing contract with selected licensees. Licensee IDs are accepted values | array |
| `selected_contracts` | | Finds users who redeemed licensing contract with selected contracts. Term IDs are accepted values | array |
| `licensing_contract_redeemed_from` | | Finds users who redeemed licensing contract on or after this date. The date format is a unix timestamp | string |
| `licensing_contract_redeemed_until` | | Finds users who redeemed licensing contract on or before this date. The date format is a unix timestamp | string |
| `data_type` | | Defines searching field | array |
| `data` | | Defines search data | string |
| `has_data` | | Find users with any data | boolean |
| `has_last_access_time` | | Finds users who have last access time | boolean |
| `last_access_time_from` | | Find users which have last access time from selected date | string |
| `last_access_time_until` | | Find users which have last access time until selected date | string |
| `selected_consents_map` | | Consent public IDs are accepted values. Specified values will be used along with consent_checked parameter | array |
| `consent_checked` | | Finds users who have checked consents. Accepted values: true/false. | boolean |
| `custom_fields` | | Finds user with following custom fields | string |
| `source` | | Data source for user searching: VX or CF (id custom fields) | string |
| `invert_credit_card_will_expire` | | Find users whose cards will expire in selected dates | boolean |
| `has_emailConfirmation_required` | | Find users who either validated or not their email. | boolean |
| `consent_has_data` | | Finds users who accepted any consent. Accepted values: true/false. If this parameter is false, selected_consents_map and consent_has_data are ignored. | boolean |
| `order_by` | | Field to order by | string |
| `order_direction` | | Order direction (asc/desc) | string |
| `q` | | Search value | string |
| `offset` | (required) | Offset from which to start returning results. For 10,001 users of more, leave offset empty and use search_after instead. | integer |
| `limit` | | 100  Maximum index of returned results | integer |
| `search_after` | | Last uid from previous request, empty for first request. Recommended in case of 10,001 users or more. | string |
| `esdebug` | | esdebug | boolean |

## Endpoint Response Messages
| HTTP status | Description |
| --- | --- |
| 2 | Access denied |
| 200 | successful operation |
