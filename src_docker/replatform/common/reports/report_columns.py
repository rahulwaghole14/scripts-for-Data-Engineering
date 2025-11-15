from common.reports.map_fields import *



def get_query_column_names(query_response):
    column_names = []
    for field in query_response['metricContainer']['metrics']:
        metric = get_overall_metric_name(field['id'])
        output = metric
        if 'filters' in field:
            filter = query_response['metricContainer']['metricFilters'][int(field['filters'][0])]
            if filter['type'] == 'segment':
                output = output + ' - Segment: ' + get_segment_name(filter['segmentId'])
            elif filter['type'] == 'breakdown':
                output = output + ' - Breakdown: ' + get_dimension_name(filter['dimension'].split("/")[1]) + " - " + filter['itemId']
            else:
                print(filter['type'] + "missing from report")
        column_names.append(output)
    return column_names




