srp_editorial_metrics_query = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65a07f1d7512d11b24e11377"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-01-01T00:00:00.000/2024-02-01T00:00:00.000",
            "dateRangeId": "thisMonth"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visitors"
            },
            {
                "columnId": "1",
                "id": "metrics/pageviews"
            },
            {
                "columnId": "3",
                "id": "metrics/visitors",
                "filters": [
                    "0"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visitors",
                "filters": [
                    "1"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visitors",
                "filters": [
                    "2"
                ]
            },
            {
                "columnId": "6",
                "id": "metrics/visitors",
                "filters": [
                    "3"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visitors",
                "filters": [
                    "4"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visitors",
                "filters": [
                    "5"
                ]
            },
            {
                "columnId": "9",
                "id": "metrics/visitors",
                "filters": [
                    "6"
                ]
            },
            {
                "columnId": "10",
                "id": "metrics/visitors",
                "filters": [
                    "7"
                ]
            },
            {
                "columnId": "11",
                "id": "metrics/visitors",
                "filters": [
                    "8"
                ]
            },
            {
                "columnId": "12",
                "id": "metrics/visitors",
                "filters": [
                    "9"
                ]
            },
            {
                "columnId": "13",
                "id": "metrics/visitors",
                "filters": [
                    "10"
                ]
            },
            {
                "columnId": "15",
                "id": "metrics/pageviews",
                "filters": [
                    "11"
                ]
            },
            {
                "columnId": "16",
                "id": "metrics/pageviews",
                "filters": [
                    "12"
                ]
            },
            {
                "columnId": "17",
                "id": "metrics/pageviews",
                "filters": [
                    "13"
                ]
            },
            {
                "columnId": "18",
                "id": "metrics/pageviews",
                "filters": [
                    "14"
                ]
            },
            {
                "columnId": "19",
                "id": "metrics/event4"
            },
            {
                "columnId": "20",
                "id": "metrics/entries"
            },
            {
                "columnId": "21",
                "id": "metrics/bounces"
            },
            {
                "columnId": "22",
                "id": "metrics/bouncerate"
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_6387dd52e25ee51ba730b737"
            },
            {
                "id": "1",
                "type": "segment",
                "segmentId": "s200000657_63bcd6ccc317784dc7ebcfdf"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_63bcd7397d257260f9e89500"
            },
            {
                "id": "3",
                "type": "segment",
                "segmentId": "s200000657_63bcd6aec317784dc7ebcfde"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_638d374a18d6c815da13d3ed"
            },
            {
                "id": "5",
                "type": "segment",
                "segmentId": "s200000657_63bcd6eec317784dc7ebcfe2"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_63bcd75d272c17393bd6d971"
            },
            {
                "id": "7",
                "type": "segment",
                "segmentId": "s200000657_63bcd64c8afe79194e987b05"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_638d36ecc415476aa8b8651f"
            },
            {
                "id": "9",
                "type": "segment",
                "segmentId": "s200000657_63bcd71470388b516def2812"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_65a479a1611cda6bfa3e51dd"
            },
            {
                "id": "11",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "6"
            },
            {
                "id": "12",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "13",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "2"
            },
            {
                "id": "14",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            }
        ]
    },
    "dimension": "variables/evar6",
    "anchorDate": "2024-01-01T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 5,
        "page": 0,
        "nonesBehavior": "return-nones"
    },
    "capacityMetadata": {
        "associations": [
            {
                "name": "applicationName",
                "value": "Analysis Workspace UI"
            },
            {
                "name": "projectId",
                "value": "undefined"
            },
            {
                "name": "projectName",
                "value": "New project"
            },
            {
                "name": "panelName",
                "value": "Freeform table"
            }
        ]
    }
}

srp_editorial_metrics_query_no_referrer = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65a07f1d7512d11b24e11377"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-01-15T00:00:00.000/2024-01-16T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visitors",
                "sort": "desc"
            },
            {
                "columnId": "1",
                "id": "metrics/pageviews"
            },
            {
                "columnId": "3",
                "id": "metrics/visitors",
                "filters": [
                    "0"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visitors",
                "filters": [
                    "1"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visitors",
                "filters": [
                    "2"
                ]
            },
            {
                "columnId": "6",
                "id": "metrics/visitors",
                "filters": [
                    "3"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visitors",
                "filters": [
                    "4"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visitors",
                "filters": [
                    "5"
                ]
            },
            {
                "columnId": "9",
                "id": "metrics/visitors",
                "filters": [
                    "6"
                ]
            },
            {
                "columnId": "10",
                "id": "metrics/visitors",
                "filters": [
                    "7"
                ]
            },
            {
                "columnId": "11",
                "id": "metrics/visitors",
                "filters": [
                    "8"
                ]
            },
            {
                "columnId": "12",
                "id": "metrics/visitors",
                "filters": [
                    "9"
                ]
            },
            {
                "columnId": "13",
                "id": "metrics/visitors",
                "filters": [
                    "10"
                ]
            },
            {
                "columnId": "15",
                "id": "metrics/pageviews",
                "filters": [
                    "11"
                ]
            },
            {
                "columnId": "16",
                "id": "metrics/pageviews",
                "filters": [
                    "12"
                ]
            },
            {
                "columnId": "17",
                "id": "metrics/pageviews",
                "filters": [
                    "13"
                ]
            },
            {
                "columnId": "18",
                "id": "metrics/event4"
            },
            {
                "columnId": "19",
                "id": "metrics/entries"
            },
            {
                "columnId": "20",
                "id": "metrics/bounces"
            },
            {
                "columnId": "21",
                "id": "metrics/bouncerate"
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_6387dd52e25ee51ba730b737"
            },
            {
                "id": "1",
                "type": "segment",
                "segmentId": "s200000657_63bcd6ccc317784dc7ebcfdf"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_63bcd7397d257260f9e89500"
            },
            {
                "id": "3",
                "type": "segment",
                "segmentId": "s200000657_63bcd6aec317784dc7ebcfde"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_638d374a18d6c815da13d3ed"
            },
            {
                "id": "5",
                "type": "segment",
                "segmentId": "s200000657_63bcd6eec317784dc7ebcfe2"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_63bcd75d272c17393bd6d971"
            },
            {
                "id": "7",
                "type": "segment",
                "segmentId": "s200000657_63bcd64c8afe79194e987b05"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_638d36ecc415476aa8b8651f"
            },
            {
                "id": "9",
                "type": "segment",
                "segmentId": "s200000657_63bcd71470388b516def2812"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_65a479a1611cda6bfa3e51dd"
            },
            {
                "id": "11",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "6"
            },
            {
                "id": "12",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "13",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "2"
            }
        ]
    },
    "dimension": "variables/evar6",
    "anchorDate": "2024-01-15T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 5,
        "page": 0,
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
                "value": "65a47a0e5e45f45618f82e1d"
            },
            {
                "name": "projectName",
                "value": "Keith Report - Dev (SRP)"
            },
            {
                "name": "panelName",
                "value": "Freeform table"
            }
        ]
    }
}


editorial_metrics_query ={
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-08-09T00:00:00.000/2023-08-10T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/visitors"
            },
            {
                "columnId": "1",
                "id": "cm200000657_5b886cae98ca3a1964a4b937"
            },
            {
                "columnId": "3",
                "id": "metrics/visitors",
                "filters": [
                    "0"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/visitors",
                "filters": [
                    "1"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/visitors",
                "filters": [
                    "2"
                ]
            },
            {
                "columnId": "6",
                "id": "metrics/visitors",
                "filters": [
                    "3"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visitors",
                "filters": [
                    "4"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visitors",
                "filters": [
                    "5"
                ]
            },
            {
                "columnId": "9",
                "id": "metrics/visitors",
                "filters": [
                    "6"
                ]
            },
            {
                "columnId": "10",
                "id": "metrics/visitors",
                "filters": [
                    "7"
                ]
            },
            {
                "columnId": "11",
                "id": "metrics/visitors",
                "filters": [
                    "8"
                ]
            },
            {
                "columnId": "12",
                "id": "metrics/visitors",
                "filters": [
                    "9"
                ]
            },
            {
                "columnId": "13",
                "id": "metrics/visitors",
                "filters": [
                    "10"
                ]
            },
            {
                "columnId": "15",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "11"
                ]
            },
            {
                "columnId": "16",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "12"
                ]
            },
            {
                "columnId": "17",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "sort": "desc",
                "filters": [
                    "13"
                ]
            },
            {
                "columnId": "18",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "14"
                ]
            },
            {
                "columnId": "19",
                "id": "cm200000657_55946057e4b08bf55c62385f"
            },
            {
                "columnId": "20",
                "id": "metrics/entries"
            },
            {
                "columnId": "21",
                "id": "metrics/bounces"
            },
            {
                "columnId": "22",
                "id": "metrics/bouncerate"
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_6387dd52e25ee51ba730b737"
            },
            {
                "id": "1",
                "type": "segment",
                "segmentId": "s200000657_63bcd6ccc317784dc7ebcfdf"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_63bcd7397d257260f9e89500"
            },
            {
                "id": "3",
                "type": "segment",
                "segmentId": "s200000657_63bcd6aec317784dc7ebcfde"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_638d374a18d6c815da13d3ed"
            },
            {
                "id": "5",
                "type": "segment",
                "segmentId": "s200000657_63bcd6eec317784dc7ebcfe2"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_63bcd75d272c17393bd6d971"
            },
            {
                "id": "7",
                "type": "segment",
                "segmentId": "s200000657_63bcd64c8afe79194e987b05"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_638d36ecc415476aa8b8651f"
            },
            {
                "id": "9",
                "type": "segment",
                "segmentId": "s200000657_63bcd71470388b516def2812"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_646fe83ac1f0f97f09c4c875"
            },
            {
                "id": "11",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "6"
            },
            {
                "id": "12",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "3"
            },
            {
                "id": "13",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "2"
            },
            {
                "id": "14",
                "type": "breakdown",
                "dimension": "variables/referrertype",
                "itemId": "9"
            }
        ]
    },
    "dimension": "variables/prop11",
    "anchorDate": "2023-09-06T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 10000,
        "page": 0,
        "nonesBehavior": "exclude-nones"
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
                "value": "64f903608ec4721d9f1a83d9"
            },
            {
                "name": "projectName",
                "value": "Keith Report - Dev"
            },
            {
                "name": "panelName",
                "value": "Freeform table"
            }
        ]
    }
}




