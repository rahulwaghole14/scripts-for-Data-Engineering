-- this script is for updating existing matrix hightouch modle from the sql server CDW database
-------------------------


USE STAGE;

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--------------------
-- CD-1077 
-- There are three tables need to be updated:hightouch_user_profiles_all, hightouch_user_profiles_match_idm, hightouch_user_profiles_no_match
-- Make Deceased a nested attribute print__deceased boolean and set to true in user profile braze
-- set print__do_not_contact to true so that we stop contacting deceased.
--------------------------------------------------------
ALTER TABLE [matrix].[hightouch_user_profiles_match_idm]
ADD print__deceased INT NULL;

ALTER TABLE [matrix].[hightouch_user_profiles_no_match]
ADD print__deceased INT NULL;

ALTER TABLE [matrix].[hightouch_user_profiles_all]
ADD print__deceased INT NULL;

-- For dropping the columns
-- ALTER TABLE [matrix].[hightouch_user_profiles_match_idm]
-- DROP COLUMN print__deceased;

-- ALTER TABLE [matrix].[hightouch_user_profiles_no_match]
-- DROP COLUMN print__deceased;

-- ALTER TABLE [matrix].[hightouch_user_profiles_all]
-- DROP COLUMN print__deceased;


