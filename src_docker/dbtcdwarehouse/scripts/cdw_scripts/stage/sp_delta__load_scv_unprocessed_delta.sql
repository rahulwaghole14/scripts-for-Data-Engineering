SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [delta].[load_scv_unprocessed_delta] @record_size int AS
	declare @size int
	-- Usage
	-- exec [stage].[delta].[load_scv_unprocessed_delta] @record_size;


	insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'started');

    select top 1000 * into #unprocessed_delta_staging from [stage].[delta].[scv_unprocessed_delta]
	where delta_process_dts is null
	/*
	delete from [scv].[delta].[scv_delta] where record_source = 'FIBRE';
	delete from [stage].[delta].[scv_delta_processing] where record_source = 'FIBRE';
	*/

	alter index all on [scv].[delta].[SCV_delta] disable;

	Begin Try
	 Begin transaction unprocessDeltaIns

	    update [stage].[delta].[scv_unprocessed_delta]
		 set delta_process_dts = getdate()
		where customer_hash in (select customer_hash from #unprocessed_delta_staging)
	-- loading delta records from delta_unprocessed_delta
		insert into [scv].[delta].[SCV_delta]
		select * from #unprocessed_delta_staging uds where not exists (select * from scv.dbo.scv scv where scv.[Customer Hash] = uds.customer_hash)
     Commit transaction unprocessDeltaIns
	End Try

	Begin Catch
		if (@@TRANCOUNT > 0)
		  begin
			Rollback transaction unprocessDeltaIns
		  end
	End Catch
alter index all on [scv].[delta].[SCV_delta] rebuild;

insert into [stage].[log].[etl_job] ([user],[source],[job],[status]) values (CURRENT_USER,OBJECT_SCHEMA_NAME(@@PROCID),OBJECT_NAME(@@PROCID),'finished');
GO
