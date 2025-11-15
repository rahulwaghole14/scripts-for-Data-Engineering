SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Michael J Lam
-- Create date: 26/02/2020
-- Description:	Allow end-user to run query on stage database table matrix.subscription
-- =============================================
CREATE PROCEDURE [dbo].[print_prod_by_channel]
	-- To specify the period that need include for the dataset
	@start_date varchar(12) = null,
	@end_date varchar(12) = null
AS
BEGIN
	Declare @processing_date date = convert(date,convert(varchar(11),getdate()))
	Declare @from_date date, @to_date date
	Declare @from_year int, @to_year int

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
   -- tables used in below queries are from database [stage]


BEGIN TRY
    -- Test if the input parameters is in correct date format
  set @from_year = year(@start_date)
  set @to_year = year(@end_date)
  set @from_date = @start_date
  set @to_date = @end_date
     -- check if input parameters contain values
  if @start_date = null or @end_date = null
	begin
	 RAISERROR('This program expected a start and end date to be supplied',16,1);
	 PRINT('This program expected a start and end date immediately following the name of the command')
	 PRINT('Usage example --> Exec print_prod_by_channel <YYYY-MM-DD>,<YYYY-MM-DD>')
	 RETURN(-1)
	end;
	/****** Product subscription per Channel  ******/
WITH prod_channel(YearMonth,SourceID,Productid,sub_made) as
	(SELECT YearMonth, SourceId, productid, count(order_type)
	 FROM (SELECT Year(sord_entry_date)*100+month(sord_entry_date) as YearMonth
				,[SourceID]
				,[productid]
				,[order_type]
				FROM [stage].[matrix].[subscription]
				WHERE convert(date, cast([sord_entry_date] as varchar(11)))
				 between isnull(@from_date, @processing_date) and isnull(@to_date, @processing_date)) sub
	 GROUP BY YearMonth,sourceid,ProductID)
SELECT YearMonth,
	tp.ProductID,
	ProductDesc,
-- aggregate subscription qty across pre-determined 14 sales channel
	sum(case when lower(sourceid) = 'phone' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) Phone,

	sum(case when lower(sourceid) = 'm4g' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) M4G,

	sum(case when lower(sourceid) = 'email' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) Email,

	sum(case when lower(sourceid) = 'fax' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) Fax,

	sum(case when lower(sourceid) = 'ivr' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) IVR,

		sum(case when lower(sourceid) = 'magisub' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) MAGISUB,

		sum(case when lower(sourceid) = 'magsother' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) MagSother,

		sum(case when lower(sourceid) = 'mcorp' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) MCorp,

			sum(case when lower(sourceid) = 'message' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) 'Message',

			sum(case when lower(sourceid) = 'phooe' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) PHOOE,

			sum(case when lower(sourceid) = 'pione' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) PIOne,

			sum(case when lower(sourceid) = 'qhone' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) QHone,

			sum(case when lower(sourceid) = 'mcorp' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) MCorp,

			sum(case when lower(sourceid) = 'subsplus' and prod_channel.ProductID = tp.ProductID
	then prod_channel.sub_made else 0 end) SubsPlus

from prod_channel inner join [stage].[matrix].[tbl_product] tp on (tp.ProductID = prod_channel.ProductID)
Group by YearMonth, tp.ProductID, ProductDesc
order by 1,2

/** Product subscription breakdown per dimension **/

SELECT COALESCE(cast(YearMonth as varchar),'.Across All Period.') YearMonth
,COALESCE(SourceId,'.Across All Channels.') Channel
,COALESCE(productid,'.Across All Products.') Productid
,count(order_type) Sub_made
	 FROM (SELECT Year(sord_entry_date)*100+month(sord_entry_date) as YearMonth
				,[SourceID]
				,[productid]
				,[order_type]
				FROM [stage].[matrix].[subscription]
				WHERE convert(date, cast([sord_entry_date] as varchar(11)))
				 between isnull(@from_date, @processing_date)
				 and isnull(@to_date, @processing_date)) sub
	GROUP BY CUBE(YearMonth,sourceid,ProductID)
	ORDER BY 1,2,3
END TRY

BEGIN CATCH
    declare @Errmsg varchar(128) = ERROR_MESSAGE()
	RAISERROR('Expected input dateformat in YYYY-MM-DD',16,1, @Errmsg)
END CATCH;

/** Validate what sales channel is captured in matrix **/
select distinct lower(sourceid) channel_in_matrix from [stage].matrix.subscription
END
GO
