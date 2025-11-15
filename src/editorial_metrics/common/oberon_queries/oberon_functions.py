

def update_query_datetime(query, start_date, end_date, start_time = '00:00:00.000', end_time = '00:00:00.000'):
    date_output_string = start_date + "T" + start_time + "/" + end_date + "T" + end_time
    for index, filter in enumerate(query['globalFilters']):
        if filter['type'] == 'dateRange':
            query['globalFilters'][index]['dateRange'] = date_output_string

def update_query_pagination(query, pagination = 0):
    for index, setting in enumerate(query['settings']):
        if setting == 'page':
            query['settings']['page'] = pagination

def update_query_limit(query, limit = 10000):
    for index, setting in enumerate(query['settings']):
        if setting == 'limit':
            query['settings']['limit'] = limit



