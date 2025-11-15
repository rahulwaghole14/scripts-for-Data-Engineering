SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE PROC [dbo].[_FFX_SalesForceExtract_BookingSummary]

AS


CREATE TABLE #bookings (Booking INT, Insertionset INT, Insertion INT)
INSERT INTO #bookings
	SELECT booking, MIN(Insertionset) Insertionset, MAX(Insertion) AS Insertion
	FROM FactBookedInsertion I
	INNER JOIN DimInsertionDetails D ON D.DimInsertionDetailsKey = I.DimInsertionDetailsKey
	INNER JOIN DimDate DD ON DD.DimDateKey = I.DimIssueDateKey
	WHERE DD.GenDate >= DATEADD(d,-45,CONVERT(Date,GETDATE()))
	GROUP BY booking
CREATE NONCLUSTERED INDEX _#Bookings_booking ON #bookings (booking, Insertionset, Insertion)

CREATE TABLE #bookingDetails (Booking INT, BookingAccount VarChar(100), ClientAccount  VarChar(100), Caller Varchar(MAX), Caption Varchar(MAX),
							OrderNumber Varchar(MAX), User_Name VarChar(100), CloseDate VarChar(30), LastInsertionDate VarChar(30))
INSERT INTO #bookingDetails
	SELECT
		rB.Booking,
		A.account AS BookingAccount,
		CA.account AS ClientAccount,
		CASE T.InsertionCaller WHEN 'Unknown' THEN '' ELSE RTRIM(REPLACE(T.InsertionCaller,',',':')) END AS Caller,
		REPLACE(D.insertionCaption,',','')AS Caption,
		CASE D.OrderNumber WHEN 'Unknown' THEN '' ELSE REPLACE(D.OrderNumber,',','') END OrderNumber,
		SU.User_Name,
		CONVERT(Varchar,CD.GenDate,103) AS CloseDate
		,CONVERT(Varchar,DD.GenDate,103) AS LastInsertionDate
	FROM #bookings rB
	INNER JOIN DimInsertionDetails D ON D.Booking = rB.Booking
										AND D.InsertionSet = rB.InsertionSet
										AND D.Insertion = rB.Insertion
	INNER JOIN FactBookedInsertion I ON D.DimInsertionDetailsKey = I.DimInsertionDetailsKey
	INNER JOIN DimInsertionText T ON T.DimInsertionDetailsKey = I.DimInsertionDetailsKey
	INNER JOIN DimAccount A ON A.DimAccountKey = I.DimBookingAccountKey
	INNER JOIN DimAccount CA ON CA.DimAccountKey = I.DimCustomerAccountKey
	INNER JOIN DimDate DD ON DD.DimDateKey = I.DimIssueDateKey
	INNER JOIN DimDate CD ON CD.DimDateKey = I.DimBookingCreateDateKey
	INNER JOIN DimUser U ON U.DimUserKey = I.DimSalesRepUserKey
	INNER JOIN GeneraOffline.dbo.users SU ON SU.user_id =U.UserId
	WHERE DD.GenDate >= DATEADD(d,-45,CONVERT(Date,GETDATE()))
	AND A.AccountType NOT IN ('Target Clubs','Target SL','House','Staff','City Express','Charity')
	AND A.AccountRegion <> 'Target'
	AND D.InsertionStyle IN ('Class Comp Child', 'Classified Composite', 'Insert', 'Online', 'ROP', 'ROP Comp Child', 'ROP Composite', 'ROP Styles', 'Semi Display')
	AND I.NetRevenueValue <> 0
	CREATE NONCLUSTERED INDEX _#bookingDetails_booking ON #bookingDetails (booking)




SELECT --DISTINCT
	rD.booking,
	rD.BookingAccount,
	rD.ClientAccount,
	rD.User_Name,
	rD.Caller,
	rD.Caption,
	rD.OrderNumber,
	SUM(I.NetRevenueValue) AS Price,
	SUM(CASE WHEN D.InsertionBilled = 'Yes' THEN I.NetRevenueValue ELSE 0 END) AS Billed_amount,
	CONVERT(Varchar,Min(DD.GenDate),103)  FirstInsertionDate,
	rD.LastInsertionDate,
	rD.CloseDate,
	COUNT(*) OVER () Import_Count,
	CONVERT(Varchar,CONVERT(date, GETDATE()),103) AS ImportDate
FROM #bookingDetails rD
INNER JOIN DimInsertionDetails D ON D.Booking = rD.Booking
INNER JOIN FactBookedInsertion I ON D.DimInsertionDetailsKey = I.DimInsertionDetailsKey
INNER JOIN DimDate DD ON DD.DimDateKey = I.DimIssueDateKey
--WHERE D.Booking = 5666459
--AND I.DimTakenByUserKey <> 4906 - excluded for now... this is the cybersell web user
GROUP BY
	rD.booking,
	rD.BookingAccount,
	rD.ClientAccount,
	rD.User_Name,
	rD.Caller,
	rD.Caption,
	rD.OrderNumber,
	rD.LastInsertionDate,
	rD.CloseDate

DROP TABLE #bookings
DROP TABLE #bookingDetails


GO
