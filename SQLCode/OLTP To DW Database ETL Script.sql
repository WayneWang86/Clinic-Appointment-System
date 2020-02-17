--*************************************************************************--
-- Title: Final Project: ETL from OLTP to DW Database
-- Author: Wayne Wang
-- Desc: This file creates 'Flush and Fill' ETL process as well as 
-- an Incremental ETL process from OLTP to DW Database
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang, Created File
--**************************************************************************--
USE DWClinicReportData;
go

If Exists(Select * from Sys.objects where Name = 'pETLDropForeignKeyConstraints')
   Drop Procedure pETLDropForeignKeyConstraints;
go

Create Procedure pETLDropForeignKeyConstraints
/* Author: Wayne Wang
** Desc: Removed FKs before truncation of the tables
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for Droping Foreign Key Constraints.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	-- Drop the Foreign Key Constraints from FactDoctorShifts Table --
    Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Drop Constraint fkFactDoctorShiftsToDimDates;

	Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Drop Constraint fkFactDoctorShiftsToDimClinics;

	Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Drop Constraint fkFactDoctorShiftsToDimShifts;

	Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Drop Constraint fkFactDoctorShiftsToDimDoctors;
	
	-- Drop the Foreign Key Constraints from FactVisits Table --
	Alter Table DWClinicReportData.dbo.FactVisits
		Drop Constraint fkFactVisitsToDimDates;

	Alter Table DWClinicReportData.dbo.FactVisits
		Drop Constraint fkFactVisitsToDimClinics;

	Alter Table DWClinicReportData.dbo.FactVisits
		Drop Constraint fkFactVisitsToDimPatients;

	Alter Table DWClinicReportData.dbo.FactVisits
		Drop Constraint fkFactVisitsToDimDoctors;

	Alter Table DWClinicReportData.dbo.FactVisits
		Drop Constraint fkFactVisitsToDimProcedures;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLDropForeignKeyConstraints;
 Print @Status;
*/

If Exists(Select * from Sys.objects where Name = 'pETLTruncateTables')
   Drop Procedure pETLTruncateTables;
go

Create Procedure pETLTruncateTables
/* Author: Wayne Wang
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for Truncating the tables
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    TRUNCATE TABLE DWClinicReportData.[dbo].[DimClinics];
	TRUNCATE TABLE DWClinicReportData.[dbo].[DimDates];
	TRUNCATE TABLE DWClinicReportData.[dbo].[DimDoctors];
	TRUNCATE TABLE DWClinicReportData.[dbo].[DimPatients];
	TRUNCATE TABLE DWClinicReportData.[dbo].[DimProcedures];
	TRUNCATE TABLE DWClinicReportData.[dbo].[DimShifts];
	TRUNCATE TABLE DWClinicReportData.[dbo].[FactDoctorShifts];
	TRUNCATE TABLE DWClinicReportData.[dbo].[FactVisits];
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLTruncateTables;
 Print @Status;
*/

If Exists(Select * from Sys.objects where Name = 'vETLDimClinics')
   Drop View vETLDimClinics;
go

CREATE VIEW vETLDimClinics
AS
	SELECT [ClinicID]    = CASE [ClinicID] WHEN 1 THEN 100 WHEN 2 THEN 200 WHEN 3 THEN 300 END
		  ,[ClinicName]  = [ClinicName]
		  ,[ClinicCity]  = [City]
		  ,[ClinicState] = [State]
		  ,[ClinicZip]   = [Zip]
	FROM   DoctorsSchedules.dbo.Clinics
GO


If Exists(Select * from Sys.objects where Name = 'pETLFillDimClinics')
   Drop Procedure pETLFillDimClinics;
go

Create Procedure pETLFillDimClinics
/* Author: Wayne Wang
** Desc: Inserts data into DimClinics
** Change Log: When,Who,What
** 2019-08-26,Wayne Wang, Created Procedure for ETL process for DimClinics table
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
     IF ((Select Count(*) From DimClinics) = 0)
     Begin
		INSERT INTO DWClinicReportData.dbo.DimClinics
		   ([ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip])
		SELECT
			[ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip]
		 FROM vETLDimClinics
	 End
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


If Exists(Select * from Sys.objects where Name = 'pETLFillDimDates')
   Drop Procedure pETLFillDimDates;
go

Create Procedure pETLFillDimDates
/* Author: Wayne Wang
** Desc: Inserts data into DimDates
** Change Log: When,Who,What
** 2019-08-26, Wayng Wang, Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
      Delete From DimDates; -- Clears table data with the need for dropping FKs
      Declare @StartDate datetime = '01/01/2004'
      Declare @EndDate datetime = '12/31/2020' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
	  SET IDENTITY_INSERT DimDates ON
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row into the date dimension table for this date
       Insert Into DimDates 
       ([DateKey], [FullDate], [FullDateName], [MonthID], [MonthName], [YearID], [YearName] )
       Values ( 
        Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
		,@DateInProcess -- [FullDate]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [FullDateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Year(@DateInProcess) -- [YearID] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End
	  SET IDENTITY_INSERT DimDates OFF
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

If Exists(Select * from Sys.objects where Name = 'vETLDimDoctors')
   Drop View vETLDimDoctors;
go

CREATE VIEW vETLDimDoctors
AS
	SELECT [DoctorID]	        = [DoctorID]
		  ,[DoctorFullName]		= CAST(CONCAT([FirstName],  ' ',[LastName]) as nvarchar(200))
		  ,[DoctorEmailAddress] = [EmailAddress]
		  ,[DoctorCity]			= [City]
		  ,[DoctorState]		= [State]
		  ,[DoctorZip]			= [Zip]
	FROM   DoctorsSchedules.dbo.Doctors
GO

If Exists(Select * from Sys.objects where Name = 'pETLFillDimDoctors')
   Drop Procedure pETLFillDimDoctors;
go

Create Procedure pETLFillDimDoctors
/* Author: Wayne Wang
** Desc: Inserts data into DimDoctors
** Change Log: When,Who,What
** 2019-08-26,Wayne Wang, Created Procedure for filling the DimDoctors table
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    INSERT INTO DimDoctors 
		  ([DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip])
    Select [DoctorID], [DoctorFullName], [DoctorEmailAddress], [DoctorCity], [DoctorState], [DoctorZip]
	FROM   vETLDimDoctors
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

If Exists(Select * from Sys.objects where Name = 'vETLDimPatients')
   Drop View vETLDimPatients;
go

CREATE VIEW vETLDimPatients
AS
	SELECT [PatientID]		 = [ID]
		  ,[PatientFullName] = CAST(CONCAT([Fname],  ' ', [Lname]) as varchar(100))
		  ,[PatientCity]     = CAST([City] as varchar(100))
		  ,[PatientState]    = CAST([State] as varchar(100))
		  ,[PatientZipCode]  = [Zipcode]
	FROM   Patients.dbo.Patients
GO

If Exists(Select * from Sys.objects where Name = 'pETLSyncDimPatients')
   Drop Procedure pETLSyncDimPatients;
go

Create Procedure pETLSyncDimPatients
/* Author: Wayne Wang
** Desc: Updates data in DimPatients using the vETLDimPatients view
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    -- 1) For UPDATE: Change the EndDate and IsCurrent on any added rows 
	With ChangedPatients 
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
			Except
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DimPatients
			Where  [IsCurrent] = 1 -- Needed if the value is changed back to previous value
		) UPDATE   DWClinicReportData.dbo.DimPatients 
			SET    [EndDate] = Cast(GetDate() as date)
			      ,[IsCurrent] = 0
			WHERE  [PatientID] IN (Select [PatientID] From ChangedPatients)
    ;
    -- 2)For INSERT or UPDATES: Add new rows to the table
	With AddedORChangedPatients 
		As(
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
			Except
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DimPatients
			Where  [IsCurrent] = 1 -- Needed if the value is changed back to previous value
		)	INSERT INTO DWClinicReportData.dbo.DimPatients
				  ([PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode],[StartDate],[EndDate],[IsCurrent])
			SELECT
				   [PatientID]
				  ,[PatientFullName]
				  ,[PatientCity]
				  ,[PatientState]
				  ,[PatientZipCode]
			      ,[StartDate] = Cast(GetDate() as date)
			      ,[EndDate] = Null
			      ,[IsCurrent] = 1
			FROM   vETLDimPatients
		    WHERE  [PatientID] IN (Select [PatientID] From AddedORChangedPatients)
    ;

    -- 3) For Delete: Change the IsCurrent status to zero
    With DeletedPatients 
		As (
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From DimPatients
			Where  [IsCurrent] = 1 -- We do not care about row already marked zero!
 			Except            			
			Select [PatientID], [PatientFullName], [PatientCity], [PatientState], [PatientZipCode] From vETLDimPatients
   		)	UPDATE DWClinicReportData.dbo.DimPatients 
			SET    [EndDate] = Cast(GetDate() as date)
			      ,[IsCurrent] = 0
			WHERE  [PatientID] IN (Select [PatientID] From DeletedPatients)
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

If Exists(Select * from Sys.objects where Name = 'vETLDimProcedures')
   Drop View vETLDimProcedures;
go

CREATE VIEW vETLDimProcedures
AS
	SELECT [ProcedureID]     = [ID]
		  ,[ProcedureName]   = [Name]
		  ,[ProcedureDesc]   = [Desc]
		  ,[ProcedureCharge] = CAST([Charge] as MONEY)
	FROM   Patients.dbo.Procedures
GO

If Exists(Select * from Sys.objects where Name = 'pETLFillDimProcedures')
   Drop Procedure pETLFillDimProcedures;
go

Create Procedure pETLFillDimProcedures
/* Author: Wayne Wang
** Desc: Inserts data into DimProcedure
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for filling the DimProcedure table
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    INSERT INTO DimProcedures 
		  ([ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge])
    Select [ProcedureID], [ProcedureName], [ProcedureDesc], [ProcedureCharge]
	FROM   vETLDimProcedures
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


If Exists(Select * from Sys.objects where Name = 'vETLDimShifts')
   Drop View vETLDimShifts;
go

CREATE VIEW vETLDimShifts
AS
	SELECT [ShiftID]    = [ShiftID]
		  ,[ShiftStart] = CASE [ShiftStart] 
							WHEN '09:00:00' THEN CAST('09:00:00' as time(0))
							WHEN '01:00:00' THEN CAST('13:00:00' as time(0))
							WHEN '21:00:00' THEN CAST('21:00:00' as time(0))
						  END
		  ,[ShiftEnd]   = CASE [ShiftEnd]  
							WHEN '05:00:00' THEN CAST('17:00:00' as time(0))
							WHEN '21:00:00' THEN CAST('21:00:00' as time(0))
							WHEN '09:00:00' THEN CAST('09:00:00' as time(0))
						  END
	FROM   DoctorsSchedules.dbo.Shifts
GO

If Exists(Select * from Sys.objects where Name = 'pETLFillDimShifts')
   Drop Procedure pETLFillDimShifts;
go

Create Procedure pETLFillDimShifts
/* Author: Wayne Wang
** Desc: Inserts data into DimShifts
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for filling the DimShifts table
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    INSERT INTO DimShifts 
		  ([ShiftID], [ShiftStart], [ShiftEnd])
    Select [ShiftID], [ShiftStart], [ShiftEnd]
	FROM  vETLDimShifts
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


If Exists(Select * from Sys.objects where Name = 'vETLFactDoctorShifts')
   Drop View vETLFactDoctorShifts;
go

CREATE VIEW vETLFactDoctorShifts
AS 
	SELECT [DoctorsShiftID] = DS.DoctorsShiftID
		  ,[ShiftDateKey]   = DD.DateKey
		  ,[ClinicKey]		= C.ClinicKey
		  ,[ShiftKey]		= S.ShiftKey
		  ,[DoctorKey]	    = D.DoctorKey
		  ,[HoursWorked]	= ABS(DATEDIFF(hh, S.ShiftStart, S.ShiftEnd))
	FROM   DoctorsSchedules.dbo.DoctorShifts AS DS
	JOIN   DimDates						     AS DD
	ON     CAST(CONVERT(NVARCHAR(50), DS.ShiftDate, 112) as int) = DD.DateKey
	JOIN   DimClinics						 AS C
	ON	   C.ClinicID = DS.ClinicID
	JOIN   DimShifts                         AS S
	ON	   S.ShiftID  = DS.ShiftID
	JOIN   DimDoctors						 AS D
	ON	   D.DoctorID = DS.DoctorID
GO

If Exists(Select * from Sys.objects where Name = 'pETLFillFactDoctorShifts')
   Drop Procedure pETLFillFactDoctorShifts;
go

Create Procedure pETLFillFactDoctorShifts
/* Author: Wayne Wang
** Desc: Inserts data into FactDoctorShifts
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for filling the FactDoctorShifts table
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    INSERT INTO FactDoctorShifts 
		  ([DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked])
    Select [DoctorsShiftID], [ShiftDateKey], [ClinicKey], [ShiftKey], [DoctorKey], [HoursWorked]
	FROM  vETLFactDoctorShifts
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

If Exists(Select * from Sys.objects where Name = 'vETLFactVisits')
   Drop View vETLFactVisits;
go

CREATE VIEW vETLFactVisits
AS
	SELECT [VisitKey]            = V.ID
		  ,[DateKey]			 = D.DateKey
		  ,[ClinicKey]			 = C.ClinicKey
		  ,[PatientKey]			 = P.PatientKey
		  ,[DoctorKey]			 = ISNULL(DC.DoctorKey, -1)
		  ,[ProcedureKey]		 = PC.ProcedureKey
		  ,[ProcedureVistCharge] = PC.ProcedureCharge
	FROM   Patients.dbo.Visits   AS  V
	JOIN   DimDates				 AS  D
	ON	   CONVERT(DATE, D.FullDate)  = CONVERT(DATE, V.[Date])
	JOIN   DimClinics			 AS  C
	ON	   C.ClinicID  = V.Clinic
	JOIN   DimPatients			 AS  P
	ON	   P.PatientID = V.Patient
	LEFT OUTER JOIN   DimDoctors			 AS  DC
	ON	   DC.DoctorID = V.Doctor
	JOIN   DimProcedures         AS  PC
	ON	   PC.ProcedureID = V.[Procedure]
GO

If Exists(Select * from Sys.objects where Name = 'pETLFillFactVisits')
   Drop Procedure pETLFillFactVisits;
go

Create Procedure pETLFillFactVisits
/* Author: Wayne Wang
** Desc: Inserts data into FactVisits
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for filling the FactVisits table
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
    INSERT INTO FactVisits 
		  ([VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge])
    Select [VisitKey], [DateKey], [ClinicKey], [PatientKey], [DoctorKey], [ProcedureKey], [ProcedureVistCharge]
	FROM   vETLFactVisits
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go


If Exists(Select * from Sys.objects where Name = 'pETLAddForeignKeyConstraints')
   Drop Procedure pETLAddForeignKeyConstraints;
go

Create Procedure pETLAddForeignKeyConstraints
/* Author: Wayne Wang
** Desc: Add FKs back to the tables
** Change Log: When,Who,What
** 2019-08-26, Wayne Wang, Created Procedure for Adding Foreign Key Constraints.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try
    -- ETL Processing Code --
	-- Drop the Foreign Key Constraints from FactDoctorShifts Table --
    Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Add Constraint fkFactDoctorShiftsToDimDates
		Foreign Key (ShiftDateKey) References DimDates;

	Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Add Constraint fkFactDoctorShiftsToDimClinics
		Foreign Key (ClinicKey) References DimClinics;

	Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Add Constraint fkFactDoctorShiftsToDimShifts
		Foreign Key (ShiftKey) References DimShifts;

	Alter Table DWClinicReportData.dbo.FactDoctorShifts
		Add Constraint fkFactDoctorShiftsToDimDoctors
		Foreign Key (DoctorKey) References DimDoctors;
	
	-- Drop the Foreign Key Constraints from FactVisits Table --
	Alter Table DWClinicReportData.dbo.FactVisits
		Add Constraint fkFactVisitsToDimDates
		Foreign Key (DateKey) References DimDates;

	Alter Table DWClinicReportData.dbo.FactVisits
		Add Constraint fkFactVisitsToDimClinics
		Foreign Key (ClinicKey) References DimClinics;

	Alter Table DWClinicReportData.dbo.FactVisits
		Add Constraint fkFactVisitsToDimPatients
		Foreign Key (PatientKey) References DimPatients;

	Alter Table DWClinicReportData.dbo.FactVisits
		WITH NOCHECK
		Add Constraint fkFactVisitsToDimDoctors
		Foreign Key (DoctorKey) References DimDoctors;

	Alter Table DWClinicReportData.dbo.FactVisits
		Add Constraint fkFactVisitsToDimProcedures
		Foreign Key (ProcedureKey) References DimProcedures;
   Set @RC = +1
  End Try
  Begin Catch
   Print Error_Message()
   Set @RC = -1
  End Catch
  Return @RC;
 End
go

EXEC pETLDropForeignKeyConstraints
GO
EXEC pETLTruncateTables
GO
EXEC pETLFillDimClinics
GO
EXEC pETLFillDimDates
GO
EXEC pETLFillDimDoctors
GO
EXEC pETLFillDimProcedures
GO
EXEC pETLFillDimShifts
GO
EXEC pETLSyncDimPatients
GO
EXEC pETLFillFactVisits
GO
EXEC pETLAddForeignKeyConstraints
GO
