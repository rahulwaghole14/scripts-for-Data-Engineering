# qualtrics
qualtrics: site survey data
it surveys hexa visitors and collects their sentiment about the article i.e. 'happy', 'angry'

# schema table
- qualtrics	surveyresponse_export_log   ?
- qualtrics	surveyquestionresponses     C:\projects\Qualtrics\Qualtrics surveyresponse_to_stage.yxmd
- qualtrics	members                     C:\projects\Qualtrics\Qualtrics surveyresponse_to_stage.yxmd
- qualtrics	articles                    C:\projects\Qualtrics\Qualtrics surveyresponse_to_stage.yxmd
- qualtrics	surveyresponses             C:\projects\Qualtrics\Qualtrics surveyresponse_to_stage.yxmd
- qualtrics	survey                      C:\projects\Qualtrics\Qualtrics surveyquestion_to_stage.yxmd
- qualtrics	surveyquestion              C:\projects\Qualtrics\Qualtrics surveyquestion_to_stage.yxmd

# alteryx:
## we run two workflows in alteryx
C:\projects\Qualtrics\Qualtrics Sentiment Survey extraction controller\Qualtrics Sentiment Survey extraction controller.yxmd
### we log the run stats and continuation token in CDW prod here:
select * from stage.qualtrics.surveyresponse_export_log

## second one after the above workflow saves file into alteryx server to load the data to CDW:
C:\projects\Qualtrics\Qualtrics surveyresponse_to_stage.yxmd
