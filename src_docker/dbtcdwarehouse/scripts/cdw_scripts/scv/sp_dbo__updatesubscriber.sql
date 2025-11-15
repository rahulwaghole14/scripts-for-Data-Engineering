SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[UpdateSubscriber] AS

DECLARE
  @IKey INT,
  @Subscriber VARCHAR(20);

SELECT [IKEY Cluster ID] = @IKey
, Subscriber = @Subscriber

FROM (SELECT a.[IKEY Cluster ID]
	, CASE WHEN a.Subscriber >= 1 THEN 'Subscriber'
	ELSE 'Prospect'
	END AS Subscriber
	FROM (SELECT [IKEY Cluster ID]
		, SUM(CASE WHEN [DataSource] = 'CV' THEN 0 ELSE 1 END) AS Subscriber
	  FROM [scv].[dbo].[SCV]
	  group by [IKEY Cluster ID]) a) b
JOIN [scv].[dbo].[SCV] c ON b.[IKEY Cluster ID] = c.[IKEY Cluster ID]
	AND b.Subscriber != c.Subscriber;

UPDATE [scv].[dbo].[SCV]
SET scv.[dbo].SCV.Subscriber	=	@Subscriber
WHERE [IKEY Cluster ID] = @IKey;

UPDATE [scv].[dbo].[SCV_Harmonize]
SET scv.[dbo].SCV_Harmonize.Subscriber	=	@Subscriber
WHERE [IKEY Cluster ID] = @IKey;

GO
