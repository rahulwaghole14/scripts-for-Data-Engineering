SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[UpdateIkey] AS

UPDATE [scv].[dbo].[SCV]
SET
scv.dbo.SCV.[IKEY Cluster ID]	=	t2.[IKEY Cluster ID],
scv.dbo.SCV.[IKEY Match Status]	=	t2.[IKEY Match Status],
scv.dbo.SCV.[Last Update Date]	=	t2.[Last Update Date]
FROM scv.dbo.SCV as t1
INNER JOIN
	[scv].[delta].[SCV_ikey] as t2
ON T1.[Unique ID] = T2.[Unique ID]

GO
