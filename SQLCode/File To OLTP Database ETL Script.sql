--*************************************************************************--
-- Title: Final Project: ETL from File to OLTP
-- Author: Wayne Wang
-- Desc: This file creates an Incremental ETL process from Files to OLTP databases
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang, Created File
--**************************************************************************--
USE TempDB
GO

IF (SELECT OBJECT_ID('pCreateOrTruncateStagingTables')) IS NOT NULL 
	DROP PROCEDURE pCreateOrTruncateStagingTables
GO

GO
Create Procedure pCreateOrTruncateStagingTables
/* Author: Wayne Wang
** Desc: Flushes all date from the staging tables 
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for Truncating the tables
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	SET NOCOUNT ON

	-- Create or truncate NewBellevueVisitsStaging table --
	IF (SELECT OBJECT_ID('NewBellevueVisitsStaging')) IS NOT NULL 
		TRUNCATE TABLE TempDB.dbo.NewBellevueVisitsStaging
	ELSE
		CREATE TABLE TempDB.dbo.NewBellevueVisitsStaging (
			[Time]      varchar(100) NULL
		   ,[Patient]	varchar(100) NULL
		   ,[Doctor]	varchar(100) NULL
		   ,[Procedure] varchar(100) NULL
		   ,[Charge]    varchar(100) NULL
		)	

	-- Create or truncate NewKirklandVisitsStaging table --
	IF (SELECT OBJECT_ID('NewKirklandVisitsStaging')) IS NOT NULL 
		TRUNCATE TABLE TempDB.dbo.NewKirklandVisitsStaging
	ELSE
		CREATE TABLE TempDB.dbo.NewKirklandVisitsStaging (
			[Time]      varchar(100) NULL
		   ,[Patient]	varchar(100) NULL
		   ,[Clinic]	varchar(100) NULL
		   ,[Doctor]	varchar(100) NULL
		   ,[Procedure] varchar(100) NULL
		   ,[Charge]    varchar(100) NULL
		)
    
	-- Create or truncate NewRedmondVisitsStaging table --
	IF (SELECT OBJECT_ID('NewRedmondVisitsStaging')) IS NOT NULL 
		TRUNCATE TABLE TempDB.dbo.NewRedmondVisitsStaging
	ELSE
		CREATE TABLE TempDB.dbo.NewRedmondVisitsStaging (
			[Time]      varchar(100) NULL
		   ,[Clinic]	varchar(100) NULL
		   ,[Patient]	varchar(100) NULL
		   ,[Doctor]	varchar(100) NULL
		   ,[Procedure] varchar(100) NULL
		   ,[Charge]    varchar(100) NULL
		)

   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

Exec pCreateOrTruncateStagingTables
GO


IF (SELECT OBJECT_ID('pETLCsvToStagingTables')) IS NOT NULL 
	DROP PROCEDURE pETLCsvToStagingTables
GO
-- Write a stored procedure to import the updated files into corresponding staging tables
GO
Create Procedure pETLCsvToStagingTables
/* Author: Wayne Wang
** Desc: Update the three staging tables with the imported csv files.
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	BULK INSERT TempDB.dbo.NewBellevueVisitsStaging
	FROM		'C:\DataFiles\Bellevue\20100102Visits.csv'
	WITH		(FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2)

	BULK INSERT TempDB.dbo.NewKirklandVisitsStaging
	FROM		'C:\DataFiles\Kirkland\20100102Visits.csv'
	WITH		(FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2)

		BULK INSERT TempDB.dbo.NewRedmondVisitsStaging
	FROM		'C:\DataFiles\Redmond\20100102Visits.csv'
	WITH		(FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2)
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

EXEC pETLCsvToStagingTables
GO


USE Patients
GO

-- Create a view for the new visits.
IF (SELECT OBJECT_ID('vETLNewVisits')) IS NOT NULL 
	DROP VIEW vETLNewVisits
GO

CREATE VIEW vETLNewVisits
AS
	SELECT [Time], [Clinic] = 1, [Patient], [Doctor], [Procedure], [Charge]
	FROM   TempDB.dbo.NewBellevueVisitsStaging
	UNION ALL
	SELECT [Time], [Clinic], [Patient], [Doctor], [Procedure], [Charge]
	FROM   TempDB.dbo.NewKirklandVisitsStaging
	UNION ALL
	SELECT [Time], [Clinic], [Patient], [Doctor], [Procedure], [Charge]
	FROM   TempDB.dbo.NewRedmondVisitsStaging
GO

-- Create a view for the new visits.
IF (SELECT OBJECT_ID('pETLSyncPatientNewVisits')) IS NOT NULL 
	DROP Procedure pETLSyncPatientNewVisits
GO

Create Procedure pETLSyncPatientNewVisits
/* Author: Wayne Wang
** Desc: Incremental ETL process from the view
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
		With NewVisits 
		As (
			Select   [Date]      = CAST(CONCAT('2010-01-02 ', CAST([time] as varchar(50))) as datetime)
				    ,[Clinic]    = CASE CAST([Clinic] as int) WHEN 1 THEN 100 WHEN 2 THEN 200 WHEN 3 THEN 300 END
				    ,[Patient]   = CAST([Patient] as int)
				    ,[Doctor]    = CAST([Doctor] as int)
				    ,[Procedure] = CAST([Procedure] as int) 
				    ,[Charge]    = CAST([Charge] as money)
			From     [dbo].[vETLNewVisits]
			Except
			Select   [Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge] 
			From     [Patients].[dbo].[Visits]
		) 
			INSERT   [Patients].[dbo].[Visits]
				     ([Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge])
		    SELECT   [Date], [Clinic], [Patient], [Doctor], [Procedure], [Charge]
			FROM     NewVisits
			ORDER BY 1,2,3,4,5,6
		;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

EXEC pETLSyncPatientNewVisits
GO
