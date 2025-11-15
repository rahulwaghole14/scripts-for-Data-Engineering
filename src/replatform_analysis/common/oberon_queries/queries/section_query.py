srp_section_query = {
    "rsid": "fairfaxnzhexa-new-replatform",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_65a05e517512d11b24e1133c"
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
                "id": "metrics/pageviews"
            },
            {
                "columnId": "1",
                "id": "cm200000657_55dbeca8e4b02ee768224469"
            },
            {
                "columnId": "2",
                "id": "metrics/visitors"
            }
        ]
    },
    "dimension": "variables/sitesections",
    "anchorDate": "2024-01-01T00:00:00",
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

old_section_query = {
    "rsid": "fairfaxnz-hexaoverall-production",
    "globalFilters": [
        {
            "type": "segment",
            "segmentId": "s200000657_566f38e9e4b080432935e591"
        },
        {
            "type": "dateRange",
            "dateRange": "2024-01-01T00:00:00.000/2024-01-18T00:00:00.000"
        }
    ],
    "metricContainer": {
        "metrics": [
            {
                "columnId": "0",
                "id": "cm200000657_5b886cae98ca3a1964a4b937",
                "sort": "desc"
            },
            {
                "columnId": "1",
                "id": "cm200000657_55dbeca8e4b02ee768224469"
            },
            {
                "columnId": "2",
                "id": "metrics/visitors"
            }
        ]
    },
    "dimension": "variables/prop2",
    "anchorDate": "2024-01-01T00:00:00",
    "settings": {
        "countRepeatInstances": True,
        "includeAnnotations": True,
        "limit": 400,
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


