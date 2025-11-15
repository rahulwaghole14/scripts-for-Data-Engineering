''' schema of table "idm"."drupal__user_profiles" '''
SCHEMA_DRUPAL__USER_PROFILES = {
    'user_id': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'id'
    },
    'adobe_id': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'externalId'
    },
    'subscriber_id': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.subscriberId'
    },
    'username': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'userName'
    },
    'record_source': {
        'type': 'char',
        'length': 32,
        'default': 'NULL'
    },
    'active': {
        'type': 'smallinteger',
        'length': 1,
        'default': 'NULL',
        'source': 'active'
    },
    'country': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'addresses.country'
    },
    'postcode': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'addresses.postalCode'
    },
    'street_address': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'addresses.primary'
    },
    'display_name': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'displayName'
    },
    'emails_primary': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'emails.primary'
    },
    'emails_type': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'emails.type'
    },
    'email': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'emails.value'
    },
    'resource_type': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'meta.resourceType'
    },
    'last_name': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'name.familyName'
    },
    'first_name': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'name.givenName'
    },
    'phone_type': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'phoneNumbers.type'
    },
    'contact_phone': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'phoneNumbers.value'
    },
    # 'roles': {
    #     'type': 'varchar',
    #     'length': 255,
    #     'default': 'NULL',
    #     'source': 'roles'
    # },
    # 'schemas': {
    #     'type': 'varchar',
    #     'length': 255,
    #     'default': 'NULL',
    #     'source': 'schemas'
    # },
    'timezone': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'timezone'
    },
    'user_consent': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.consentReference'
    },
    'email_verified': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.emailVerified'
    },
    'gender': {
        'type': 'varchar',
        'length': 10,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.gender'
    },
    'mobile_verified': {
        'type': 'smallint',
        'length': 1,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.mobileNumberVerified'
    },
    'date_of_birth': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.dateOfBirth'
    },
    'created_date': {
        'type': 'datetime',
        'length': 8,
        'default': 'NULL',
        'source': 'meta.created'
    },
    'last_modified': {
        'type': 'datetime',
        'length': 8,
        'default': 'NULL',
        'source': 'meta.lastModified'
    },
    'verified_date': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.emailVerifiedDate'
    },
    'mobile_verified_date': {
        'type': 'datetime',
        'length': 8,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.mobileVerifiedDate'
    },
    'year_of_birth': {
        'type': 'int',
        'length': 4,
        'default': 'NULL',
        'source': 'urn:ietf:params:scim:schemas:extension:custom:2.0:User.yearOfBirth'
    },
    'city_region': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
    },
    'locality': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
    },
    'record_load_dts_utc': {
        'type': 'datetime',
        'length': 8,
        'default': 'NULL'
    },
    'hash_diff': {
        'type': 'varchar',
        'length': 255,
        'default': 'NULL',
    },
    'newsletter_subs': {
    },
    'marketing_id': {
    },
    'marketing_id_email': {
    }
}
