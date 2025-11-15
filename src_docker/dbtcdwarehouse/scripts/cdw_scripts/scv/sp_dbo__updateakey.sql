SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[UpdateAkey] AS

UPDATE [scv].[dbo].[SCV]
SET
scv.dbo.SCV.[AKEY Cluster ID]	=	t2.[AKEY Cluster ID],
scv.dbo.SCV.[AKEY Match Status]	=	t2.[AKEY Match Status],
scv.dbo.SCV.[Last Update Date]	=	t2.[Last Update Date]
FROM scv.dbo.SCV as t1
INNER JOIN
	[scv].[delta].[SCV_akey] as t2
ON T1.[Unique ID] = T2.[Unique ID]

GO
