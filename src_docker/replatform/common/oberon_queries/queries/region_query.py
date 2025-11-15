srp_region_query = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d574176d3a4097d4b773"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-01-17T00:00:00.000/2024-01-26T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "metrics/pageviews"
            },
            {
                "columnId": "1",
                "id": "metrics/visits"
            },
            {
                "columnId": "2",
                "id": "metrics/visitors"
            }
        ]
    },
    "dimension": "variables/daterangeday",
    "anchorDate": "2024-01-17T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
        "page": 0,
        "dimensionSort": "asc",
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

old_region_report = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d574176d3a4097d4b773"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2023-01-01T00:00:00.000/2024-01-17T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "cm200000657_5b886cae98ca3a1964a4b937"
            },
            {
                "columnId": "1",
                "id": "metrics/visits"
            },
            {
                "columnId": "2",
                "id": "metrics/visitors"
            }
        ]
    },
    "dimension": "variables/daterangeday",
    "anchorDate": "2023-01-01T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
        "page": 0,
        "dimensionSort": "asc",
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