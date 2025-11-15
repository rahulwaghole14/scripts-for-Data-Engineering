
logged_in_metrics_query = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_6462a9d48aa7294b108e672b"
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
            }
        ]
    },
    "dimension": "variables/evar14",
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



logged_in_metrics_query_srp = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
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
            }
        ]
    },
    "dimension": "variables/evar7",
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






