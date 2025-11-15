SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[UpdateDeleteTable] AS

UPDATE scv.delta.SCV_delete
SET
scv.delta.SCV_delete.Delete_Date	=	t2.Delete_Date
FROM scv.delta.SCV_delete as t1
INNER JOIN
	 scv.delta.SCV_delete_processedTime as t2
ON t1.customer_id = t2.customer_id
AND t1.record_source = t2.record_source

GO
