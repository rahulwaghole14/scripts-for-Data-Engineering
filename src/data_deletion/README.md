
# branch
feat/CD-825-manual-data-deletion-request-for-CDW-BigQuery

# Configuration

Create `.env` file with `GOOGLE_CLOUD_CRED_BASE64=`   base64 encoded GCP credentials at project root.

---

The following commands should be run at the root of the project.

**Install Python env and management tools if required:**

`brew install pyenv`

`brew install pyenv-virtualenv`

`pyenv install 3.10.18`

**Setup environment:**

`pyenv virtualenv 3.10.18 data_deletion`

`pyenv activate data_deletion`

**Install library requirements from file at project root:**

`python3 -m pip install -r requirements.txt`

# Technical Flow

1. determine list of tables that hold user data 
    >see configuration.py for table list
2. insert users to be deleted all into one temp table for efficiency
3. email, hexa_account_id, marketing_id
4. run delete scripts
5. drop temp tables

# CSV Format

CSV file provides the list of users for deletion.

The file `to_delete.csv` should be created in the /data_deletion folder.

**File format and fields:**  
email,user__id  
dp@emails.com,123  

email is string. 
user__id (hexa_account_id) is integer. 


# Deletion Process

1. Open the shared spreadsheet and look at CDW column, see if there are any new users to delete: 
     [Users to Delete](https://docs.google.com/spreadsheets/d/12aotmGtFFji37rV5c61S9aMW3dvFc5CEYoAZYqeC-Vg/)

2. Add users to the CSV file to be deleted  
    e.g. contents of csv file:  
    email,user__id  
    me@hotmail.com,123  
    you@hotmail.com,  
    blah@gmail.com,654      
    > notice that above middle user has no "hexa Account ID" which the script will handle without errors.

3. save the file in the data_deletion folder

4. run the data_deletion/main.py script

5. it will delete the users from CDW (sqlserver + bigquery)
done

## Other Notes

* eventually this will be automated, however we need to run this manually as it stands:

* for this task it is just hexa users not neighborly

### missing from data deletion script:
#### bigquery
`hexa-data-report-etl-prod.cdw_stage_matrix` table(s) need to be updated with the deleted users?
