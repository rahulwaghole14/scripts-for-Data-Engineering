

def check_bq_table(client, dataset_id, table_id):
    datasets = client.list_datasets()
    output = False
    if dataset_id not in [d.dataset_id for d in list(datasets)]:
        print("Incorrect Dataset for Project")
        quit()
    else:
        tables = client.list_tables(dataset_id) 
        if table_id in [t.table_id for t in list(tables)]:
            #print("table exists")
            output = True
        #else:
            #print("Create table")
    return output



