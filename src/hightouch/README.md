
# hightough things

# models

## data sources

### idm/am
dev__cdw_to_braze__idm_user_profiles
```json
{
  "external_ids": [
    "3F891746-0005-53CD-9EC5-64A44064E95D"
  ],
  "fields_to_export": [
    "country",
    "dob",
    "email",
    "first_name",
    "gender",
    "home_city",
    "last_name",
    "phone",
    "time_zone",
    "custom_attributes",
    "external_id"
  ]
}

{
  "users": [
    {
      "external_id": "3F891746-0005-53CD-9EC5-64A44064E95D",
      "first_name": "name",
      "last_name": "name",
      "email": "email@gmail.com",
      "custom_attributes": {
        "digital__mobile_verified": false,
        "digital__email_verified": true,
        "digital__consent": false,
        "digital__active": true,
        "digital__email_verified_at": "2023-06-13T19:59:57.000Z",
        "digital__created_date": "2023-06-14T07:58:51.000Z",
        "digital__username": "blah_234",
        "digital__display_name": "blah"
      },
      "dob": "2004-09-09T00:00:00.000Z",
      "country": "NZ",
      "gender": "M",
      "phone": "+64213393939"
    }
  ],
  "message": "success"
}
```

#### model1 idm/am

```sql

```
#### model2 idm/am
dev__cdw_to_braze__idm_sub_status
```json
{
  "subscription_group_id": "ae84b58a-8638-46b5-8a61-38c5b9d5bc0a",
  "subscription_state": "subscribed",
  "external_id": [
    "1E6B777B-C8B4-54B2-A23C-C364C337713C"
  ]
}
```
### supporter
#### model1 supporter
dev__cdw_to_braze__presspatron_user_profiles
```json
{
  "external_ids": [
    "788da151-5512-53a1-8f7e-5ec240b31526"
  ],
  "fields_to_export": [
    "first_name",
    "last_name",
    "email",
    "custom_attributes",
    "external_id"
  ]
}

{
  "users": [
    {
      "external_id": "788da151-5512-53a1-8f7e-5ec240b31526",
      "first_name": "name",
      "last_name": "name",
      "email": "email@orcon.net.nz",
      "custom_attributes": {
        "supporter__total_contribution": "10",
        "supporter__recurring_contribution": "0",
        "supporter__active": "false",
        "supporter__subscribed_to_newsletter": true,
        "supporter__sign_up_date": "2021-09-16"
      }
    }
  ],
  "message": "success"
}
```

#### model2 supporter
dev__cdw_to_braze__presspatron_sub_status
```json
{
  "subscription_group_id": "de203f29-d53c-4816-bf74-2c37708c80e0",
  "subscription_state": "subscribed",
  "external_id": [
    "788da151-5512-53a1-8f7e-5ec240b31526"
  ]
}
```

### print
dev__cdw_to_braze__matrix_user_profiles
dev__cdw_to_braze__matrix_sub_status

dev__cdw_to_braze__idm_user_profiles
{
    "mode": "upsert",
    "type": "object",
    "object": "user",
    "mappings": [
        {
            "to": "country",
            "from": "country",
            "type": "standard"
        },
        {
            "to": "dob",
            "from": "dob",
            "type": "standard"
        },
        {
            "to": "email",
            "from": "email",
            "type": "standard"
        },
        {
            "to": "first_name",
            "from": "first_name",
            "type": "standard"
        },
        {
            "to": "gender",
            "from": "gender",
            "type": "standard"
        },
        {
            "to": "home_city",
            "from": "home_city",
            "type": "standard"
        },
        {
            "to": "last_name",
            "from": "last_name",
            "type": "standard"
        },
        {
            "to": "phone",
            "from": "phone",
            "type": "standard"
        },
        {
            "to": "time_zone",
            "from": "time_zone",
            "type": "standard"
        }
    ],
    "configVersion": 2,
    "customMappings": [],
    "externalIdMapping": {
        "to": "external_id",
        "from": "external_id",
        "type": "standard"
    },
    "aliasMappings": []
}

dev__cdw_to_braze__idm_user_sub_status
{
    "mode": "upsert",
    "type": "object",
    "object": "user",
    "mappings": [],
    "configVersion": 2,
    "customMappings": [
        {
            "to": "communication_subscriptions",
            "type": "template",
            "template": "{{ row['communication_subscriptions'] | parse }}"
        }
    ],
    "externalIdMapping": {
        "to": "external_id",
        "from": "marketing_id",
        "type": "standard"
    },
    "aliasMappings": []
}

dev__cdw_to_braze__idm_sub_status (for general marketing list - change model query for id)
{
    "type": "subscription",
    "mappings": [],
    "configVersion": 2,
    "customMappings": [],
    "externalIdMapping": {
        "to": "external_id",
        "from": "marketing_id",
        "type": "standard"
    },
    "subscriptionGroupId": {
        "from": "sub_group_id"
    },
    "subscriptionGroupType": "email"
}
