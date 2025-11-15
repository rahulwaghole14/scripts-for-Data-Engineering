srp_referral_search = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-12-25T00:00:00.000/2024-01-24T00:00:00.000",
            "dateRangeId": "last30Days"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "2",
                "id": "metrics/visits",
                "filters": [
                    "0",
                    "1"
                ]
            },
            {
                "columnId": "3",
                "id": "metrics/visits",
                "filters": [
                    "2",
                    "3"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visits",
                "filters": [
                    "4",
                    "5"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visits",
                "filters": [
                    "6",
                    "7"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_566f38e9e4b080432935e591"
            },
            {
                "id": "1",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_6503a72540fb96774083b18c"
            },
            {
                "id": "3",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_6503a75b637bc95ab8907724"
            },
            {
                "id": "5",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_650cf0c1f8d3257cbba750b3"
            },
            {
                "id": "7",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            }
        ]
    },
    "dimension": "variables/daterangeday",
    "anchorDate": "2023-12-25T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
        "page": 0,
        "dimensionSort": "dsc",
        "nonesBehavior": "return-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "6503a2607d55ce23086da2e5"
            },
            {
                "name": "projectName",
                "value": "Referral Traffic"
            },
            {
                "name": "panelName",
                "value": "Search Referrals"
            }
        ]
    }
}

old_referral_traffic = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-12-25T00:00:00.000/2024-01-24T00:00:00.000",
            "dateRangeId": "last30Days"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "2",
                "id": "metrics/visits",
                "filters": [
                    "0",
                    "1"
                ]
            },
            {
                "columnId": "3",
                "id": "metrics/visits",
                "filters": [
                    "2",
                    "3"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visits",
                "filters": [
                    "4",
                    "5"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visits",
                "filters": [
                    "6",
                    "7"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_566f38e9e4b080432935e591"
            },
            {
                "id": "1",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_6503a72540fb96774083b18c"
            },
            {
                "id": "3",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_6503a75b637bc95ab8907724"
            },
            {
                "id": "5",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_650cf0c1f8d3257cbba750b3"
            },
            {
                "id": "7",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            }
        ]
    },
    "dimension": "variables/daterangeday",
    "anchorDate": "2023-12-25T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
        "page": 0,
        "dimensionSort": "dsc",
        "nonesBehavior": "return-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "6503a2607d55ce23086da2e5"
            },
            {
                "name": "projectName",
                "value": "Referral Traffic"
            },
            {
                "name": "panelName",
                "value": "Search Referrals"
            }
        ]
    }
}

old_social_referrals = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-12-25T00:00:00.000/2024-01-24T00:00:00.000",
            "dateRangeId": "last30Days"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "2",
                "id": "metrics/visits",
                "filters": [
                    "0",
                    "1"
                ]
            },
            {
                "columnId": "3",
                "id": "metrics/visits",
                "filters": [
                    "2",
                    "3"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visits",
                "filters": [
                    "4",
                    "5"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visits",
                "filters": [
                    "6",
                    "7"
                ]
            },
            {
                "columnId": "6",
                "id": "metrics/visits",
                "filters": [
                    "8",
                    "9"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visits",
                "filters": [
                    "10",
                    "11"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visits",
                "filters": [
                    "12",
                    "13"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_566f38e9e4b080432935e591"
            },
            {
                "id": "1",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_562858f4e4b0c155a4d7244f"
            },
            {
                "id": "3",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_6503a21ebdd2442634737d34"
            },
            {
                "id": "5",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_6503a304bdd2442634737d36"
            },
            {
                "id": "7",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_6503a3976d430871743343e9"
            },
            {
                "id": "9",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_5b3e88c805871b6adfcd9e9a"
            },
            {
                "id": "11",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "12",
                "type": "segment",
                "segmentId": "s200000657_6503a4d366c9661cb23aa1dc"
            },
            {
                "id": "13",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            }
        ]
    },
    "dimension": "variables/daterangeday",
    "anchorDate": "2023-12-25T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
        "page": 0,
        "dimensionSort": "dsc",
        "nonesBehavior": "return-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "6503a2607d55ce23086da2e5"
            },
            {
                "name": "projectName",
                "value": "Referral Traffic"
            },
            {
                "name": "panelName",
                "value": "Social Referrals"
            }
        ]
    }
}

srp_social_referrals = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-12-25T00:00:00.000/2024-01-24T00:00:00.000",
            "dateRangeId": "last30Days"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "2",
                "id": "metrics/visits",
                "filters": [
                    "0",
                    "1"
                ]
            },
            {
                "columnId": "3",
                "id": "metrics/visits",
                "filters": [
                    "2",
                    "3"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visits",
                "filters": [
                    "4",
                    "5"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visits",
                "filters": [
                    "6",
                    "7"
                ]
            },
            {
                "columnId": "6",
                "id": "metrics/visits",
                "filters": [
                    "8",
                    "9"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visits",
                "filters": [
                    "10",
                    "11"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visits",
                "filters": [
                    "12",
                    "13"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_566f38e9e4b080432935e591"
            },
            {
                "id": "1",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_562858f4e4b0c155a4d7244f"
            },
            {
                "id": "3",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_6503a21ebdd2442634737d34"
            },
            {
                "id": "5",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_6503a304bdd2442634737d36"
            },
            {
                "id": "7",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_6503a3976d430871743343e9"
            },
            {
                "id": "9",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_5b3e88c805871b6adfcd9e9a"
            },
            {
                "id": "11",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            },
            {
                "id": "12",
                "type": "segment",
                "segmentId": "s200000657_6503a4d366c9661cb23aa1dc"
            },
            {
                "id": "13",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            }
        ]
    },
    "dimension": "variables/daterangeday",
    "anchorDate": "2023-12-25T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
        "page": 0,
        "dimensionSort": "dsc",
        "nonesBehavior": "return-nones"
    },
    "statistics": {
        "functions": [
            "col-max",
            "col-min"
        ]
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "6503a2607d55ce23086da2e5"
            },
            {
                "name": "projectName",
                "value": "Referral Traffic"
            },
            {
                "name": "panelName",
                "value": "Social Referrals"
            }
        ]
    }
}