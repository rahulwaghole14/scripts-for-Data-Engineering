we want to get data from hexa events webflow sites into bigquery and then into braze

how to get from the webflow api
the credentials for the webflow api are in the vault in round the bays rtb webflow

we can go here and list forms and get the id,  we get the siteid from here (https://webflow.com/dashboard/sites/round-the-bays/general)
List Forms

that will give us the form id, then we can use that to get the form submissions here
List Form Submissions
Get Form Submission


after this is merged we need to deploy the new workflow `hexaevent.dockerfile` and run on daily schedule 5,6am nzt

currently the secret is not on shared services account yet as i didn't have access to it yet (requested)

this is a workflow to get form submissions data from webflow (hexaevent round the bays, central districts fielddays) and into bigquery (`hexa-data-report-etl-prod.cdw_stage_hexaevent.hexaevent_formsubmissions`) the data will merge from a temp table, and both sites will merge into the same table, we can identify different sites by viewing the `siteId` field

i tried using SQLModel and SQLAlchemy because it might give us the ability to define the validation model and schema for data all in one, however when i tried to load/merge a batch of records using SQLAlchemy it didnt work and only tried to merge one record at a time. so i had to revery back to our existing strategy (pydantic, schema from model, etc).
