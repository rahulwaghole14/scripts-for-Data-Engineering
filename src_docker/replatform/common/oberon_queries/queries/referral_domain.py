srp_referral_domain = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-01-17T00:00:00.000/2024-01-25T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "1",
                "id": "metrics/pageviews",
                "filters": [
                    "0"
                ]
            },
            {
                "columnId": "2",
                "id": "metrics/pageviews",
                "filters": [
                    "1"
                ]
            },
            {
                "columnId": "3",
                "id": "metrics/pageviews",
                "filters": [
                    "2"
                ]
            },
            {
                "columnId": "4",
                "id": "metrics/pageviews",
                "filters": [
                    "3"
                ]
            },
            {
                "columnId": "5",
                "id": "metrics/pageviews",
                "filters": [
                    "4"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visitors",
                "sort": "desc",
                "filters": [
                    "5"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visitors",
                "filters": [
                    "6"
                ]
            },
            {
                "columnId": "9",
                "id": "metrics/visitors",
                "filters": [
                    "7"
                ]
            },
            {
                "columnId": "10",
                "id": "metrics/visitors",
                "filters": [
                    "8"
                ]
            },
            {
                "columnId": "11",
                "id": "metrics/visitors",
                "filters": [
                    "9"
                ]
            },
            {
                "columnId": "13",
                "id": "cm200000657_55dbeca8e4b02ee768224469",
                "filters": [
                    "10"
                ]
            },
            {
                "columnId": "14",
                "id": "cm200000657_55dbeca8e4b02ee768224469",
                "filters": [
                    "11"
                ]
            },
            {
                "columnId": "15",
                "id": "cm200000657_55dbeca8e4b02ee768224469",
                "filters": [
                    "12"
                ]
            },
            {
                "columnId": "16",
                "id": "cm200000657_55dbeca8e4b02ee768224469",
                "filters": [
                    "13"
                ]
            },
            {
                "columnId": "17",
                "id": "cm200000657_55dbeca8e4b02ee768224469",
                "filters": [
                    "14"
                ]
            }
        ],
        "metricFilters": [
            {
                "id": "0",
                "type": "segment",
                "segmentId": "s200000657_65a05e517512d11b24e1133c"
            },
            {
                "id": "1",
                "type": "segment",
                "segmentId": "s200000657_659f687c19399819b5ed30a2"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_659f68b003df2d68a73879b0"
            },
            {
                "id": "3",
                "type": "segment",
                "segmentId": "s200000657_659f68deae4a06573082f5ff"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_659f68c97f923665dfdecbd4"
            },
            {
                "id": "5",
                "type": "segment",
                "segmentId": "s200000657_65a05e517512d11b24e1133c"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_659f687c19399819b5ed30a2"
            },
            {
                "id": "7",
                "type": "segment",
                "segmentId": "s200000657_659f68b003df2d68a73879b0"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_659f68deae4a06573082f5ff"
            },
            {
                "id": "9",
                "type": "segment",
                "segmentId": "s200000657_659f68c97f923665dfdecbd4"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_65a05e517512d11b24e1133c"
            },
            {
                "id": "11",
                "type": "segment",
                "segmentId": "s200000657_659f687c19399819b5ed30a2"
            },
            {
                "id": "12",
                "type": "segment",
                "segmentId": "s200000657_659f68b003df2d68a73879b0"
            },
            {
                "id": "13",
                "type": "segment",
                "segmentId": "s200000657_659f68deae4a06573082f5ff"
            },
            {
                "id": "14",
                "type": "segment",
                "segmentId": "s200000657_659f68c97f923665dfdecbd4"
            }
        ]
    },
    "dimension": "variables/referringdomain",
    "anchorDate": "2024-01-17T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
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

old_referral_domain = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
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
                "columnId": "1",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "sort": "desc",
                "filters": [
                    "0"
                ]
            },
            {
                "columnId": "2",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "1"
                ]
            },
            {
                "columnId": "3",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "2"
                ]
            },
            {
                "columnId": "4",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "3"
                ]
            },
            {
                "columnId": "5",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "filters": [
                    "4"
                ]
            },
            {
                "columnId": "7",
                "id": "metrics/visitors",
                "filters": [
                    "5"
                ]
            },
            {
                "columnId": "8",
                "id": "metrics/visitors",
                "filters": [
                    "6"
                ]
            },
            {
                "columnId": "9",
                "id": "metrics/visitors",
                "filters": [
                    "7"
                ]
            },
            {
                "columnId": "10",
                "id": "metrics/visitors",
                "filters": [
                    "8"
                ]
            },
            {
                "columnId": "11",
                "id": "metrics/visitors",
                "filters": [
                    "9"
                ]
            },
            {
                "columnId": "13",
                "id": "metrics/visits",
                "filters": [
                    "10"
                ]
            },
            {
                "columnId": "14",
                "id": "metrics/visits",
                "filters": [
                    "11"
                ]
            },
            {
                "columnId": "15",
                "id": "metrics/visits",
                "filters": [
                    "12"
                ]
            },
            {
                "columnId": "16",
                "id": "metrics/visits",
                "filters": [
                    "13"
                ]
            },
            {
                "columnId": "17",
                "id": "metrics/visits",
                "filters": [
                    "14"
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
                "type": "segment",
                "segmentId": "s200000657_5abbfd5298ca3a66c26c9597"
            },
            {
                "id": "2",
                "type": "segment",
                "segmentId": "s200000657_5abc1d573b991275d88d6427"
            },
            {
                "id": "3",
                "type": "segment",
                "segmentId": "s200000657_5abc1daf4f278f43f7752a39"
            },
            {
                "id": "4",
                "type": "segment",
                "segmentId": "s200000657_5abc1cf8311b65354b7f3c55"
            },
            {
                "id": "5",
                "type": "segment",
                "segmentId": "s200000657_566f38e9e4b080432935e591"
            },
            {
                "id": "6",
                "type": "segment",
                "segmentId": "s200000657_5abbfd5298ca3a66c26c9597"
            },
            {
                "id": "7",
                "type": "segment",
                "segmentId": "s200000657_5abc1d573b991275d88d6427"
            },
            {
                "id": "8",
                "type": "segment",
                "segmentId": "s200000657_5abc1daf4f278f43f7752a39"
            },
            {
                "id": "9",
                "type": "segment",
                "segmentId": "s200000657_5abc1cf8311b65354b7f3c55"
            },
            {
                "id": "10",
                "type": "segment",
                "segmentId": "s200000657_566f38e9e4b080432935e591"
            },
            {
                "id": "11",
                "type": "segment",
                "segmentId": "s200000657_5abbfd5298ca3a66c26c9597"
            },
            {
                "id": "12",
                "type": "segment",
                "segmentId": "s200000657_5abc1d573b991275d88d6427"
            },
            {
                "id": "13",
                "type": "segment",
                "segmentId": "s200000657_5abc1daf4f278f43f7752a39"
            },
            {
                "id": "14",
                "type": "segment",
                "segmentId": "s200000657_5abc1cf8311b65354b7f3c55"
            }
        ]
    },
    "dimension": "variables/referringdomain",
    "anchorDate": "2024-01-01T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 10,
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