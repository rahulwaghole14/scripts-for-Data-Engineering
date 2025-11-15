def report_string(start_date, end_date, metricFilterSegments=[]):

    metric_filter_list = []
    n = 0
    for segment in metricFilterSegments:
        filter_string = (
            '''{
            "id": "'''
            + str(n)
            + '''",
            "type": "segment",
            "segmentId": "'''
            + segment
            + """"
        }"""
        )
        metric_filter_list.append(filter_string)
        n = n + 1

    report_json = (
        '''{
        "rsid": "fairfaxnz-hexaoverall-production",
        "globalFilters": [
            {
                "type": "dateRange",
                "dateRange": "'''
        + start_date
        + """T00:00:00.000/"""
        + end_date
        + """T00:00:00.000"
            }
        ],
        "metricContainer": {
            "metrics": [
                {
                    "columnId": "1",
                    "id": "metrics/visits",
                    "filters": [
                        "0"
                    ]
                }
            ],
            "metricFilters": ["""
        + ",".join(metric_filter_list)
        + """]
        },
        "dimension": "variables/prop11",
        "settings": {
            "countRepeatInstances": true,
            "limit": 50,
            "page": 0
        }

    }
    """
    )
    return report_json
