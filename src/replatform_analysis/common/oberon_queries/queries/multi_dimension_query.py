srp_multi_dim = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d5337dd7b567b6c89565"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d4c37dd7b567b6c89564"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d5537dd7b567b6c89566"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d574176d3a4097d4b773"
        },
        {
            "type": "segment",
            "segmentId": "s200000657_65b2d5dc7dd7b567b6c89567"
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