check the field for is_matched_genera because seems to not working properly
create a file thats just got salesforceid, and genera mapping ids only


for opportunites, lets map back to Accounts > genera ids
and also potentially the Sales person id from google sheets mapping

if there is material contact on opp, we should append to Opportunities, and Account

this is where the material contacts can go for now in the SF material contact tab
https://docs.google.com/spreadsheets/d/1q9-7TgoY2vR-ZXIeEgO2sMfAxS4l39LA/edit#gid=729294815

we might use the material contact in this mapping to naviga:
https://docs.google.com/spreadsheets/d/1nJX57_SSFbF3MB-CJl8Rrr16nFLwqn8mpvmrh8jKz74/edit#gid=523497376

opportunities
exclude stage "Closed" won blah -- done

both accounts and opportunities
filter to just accounts from d365 OR all open opportunities (prospect accts with opportunities that are not closed) (closedate has to be 30 days before now and future) -- done

contacts
contacts (get a list of contacts however in naviga we cannot have same contact across different accounts because the email)


Only accounts to migrate from Salesforce that is not on the D365 list should cover all three points below:
Accounts that are not yet 'approved' by Finance (unsure of what the Finance terminology is) that are labelled as 'Prospect accounts' and must also;
have an open opportunity stage against it with a;
close date in the future or 30 days in the past. (30 days in the past is an exception in case staff have forgotten to extend their opportunity)
Give me a call if you have any questions.
