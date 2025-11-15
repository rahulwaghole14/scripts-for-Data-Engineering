""" schema map dict """

schema_map_dict = {
    "page_name_url": {
        "type": "string",
        "source": "page_name_url",
        "target": "page_name_url",
    },
    "link_name": {
        "type": "string",
        "source": "link_name",
        "target": "link_name",
    },
    "registration_submitted": {
        "type": "float",
        "source": "registration_submitted",
        "target": "registration_submitted",
    },
    "registration_start": {
        "type": "float",
        "source": "registration_start",
        "target": "registration_start",
    },
    "login_start": {
        "type": "float",
        "source": "login_start",
        "target": "login_start",
    },
    "link_click": {
        "type": "float",
        "source": "link_click",
        "target": "link_click",
    },
    "date_at": {
        "type": "date",
        "dataframe_type": "datetime64[ns]",
        "source": "date_at",
        "target": "date_at",
    },
    "date_load_at": {
        "type": "date",
        "dataframe_type": "datetime64[ns]",
        "source": "date_load_at",
        "target": "date_load_at",
    },
}
