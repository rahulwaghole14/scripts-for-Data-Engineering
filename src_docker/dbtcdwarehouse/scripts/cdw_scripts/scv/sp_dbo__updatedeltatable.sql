SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[UpdateDeltaTable] AS

UPDATE scv.delta.SCV_delta
SET
scv.delta.SCV_delta.delta_process_dts	=	t2.delta_process_dts
FROM scv.delta.SCV_delta as t1
INNER JOIN
	 scv.delta.SCV_processedTime as t2
ON t1.customer_id = t2.customer_id
AND t1.record_source = t2.record_source

GO
