 --*************************************************************************--
-- Title: Final Project: Automating Backups
-- Author: Wayne Wang
-- Desc: This file creates spros for automating the Backup of all three final databases 
-- (two OLTP database and one DW database)
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang, Created File
--**************************************************************************--

--**************************************************************************--
--- Backup for DoctorSchedules ---

--[ Pre-Backup Tasks]---------------------------------------------------------------------
-- Task 1: Create Backup Devices as needed --
If Exists (SELECT name FROM sys.objects WHERE name = N'pCreateDoctorsSchedulesBackupDevice')
Begin
  Drop Proc pCreateDoctorsSchedulesBackupDevice;
End
go

Create -- Drop
Proc pCreateDoctorsSchedulesBackupDevice
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Creates a backup device for the DoctorsSchedules DB as needed.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Backup Device Code --
    If NOT Exists (Select * From model.sys.sysdevices Where Name = N'DoctorsSchedulesBackupDevice')
      Exec master.dbo.sp_AdDumpDevice
        @devtype = N'disk'
      , @logicalname = N'DoctorsSchedulesBackupDevice'
      , @physicalname = N'C:\BackupFiles\DoctorsSchedulesBackupDevice.bak'
		-- Backup Device Code --
		Print 'Success in Creating Backing Device for the DoctorsSchedules database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error in Creating Backing Device for the DoctorsSchedules database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) Create a Folder (as needed)
!! MD C:\BackupFiles

-- Step 2) Delete backup Device (as needed)
Exec master.dbo.sp_dropdevice @logicalname = N'DoctorsSchedulesBackupDevice';

-- Step 3) Test the Sproc
Exec pCreateDoctorsSchedulesBackupDevice;
go  
****************************************************************************************************/



--[ Pre-Backup Tasks]---------------------------------------------------------------------

-- [ Backup Tasks] -----------------------------------------------------------------------
-- Task 1: Backup the DoctorsSchedules Database
Use [Master];
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pMaintBackupDoctorsSchedules')
Begin
  Drop Proc pMaintBackupDoctorsSchedules;
End
go

Create -- Drop
Proc pMaintBackupDoctorsSchedules
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Performs database backups on the DoctorsSchedules DB.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Backup Code --
    If (DatePart(dw,GetDate()) = 1)-- Sunday
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Sun-Full', Init;
    Else                                                             
    If (DatePart(dw,GetDate()) = 2)-- Monday                         
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Mon-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 3)-- Tuesday                        
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Tue-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 4)-- Wednesday                      
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Wed-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 5)-- Thursday                       
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Thu-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 6)-- Friday                         
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Fri-Full';  
    Else                                                             
    If (DatePart(dw,GetDate()) = 7)-- Saturday                       
      Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Sat-Full';
		-- Backup Code --
		Print 'Success in Backing up the DoctorsSchedules database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Backing up the DoctorsSchedules database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DoctorsSchedulesBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pMaintBackupDoctorsSchedules;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DoctorsSchedulesBackupDevice.bak';
go  
****************************************************************************************************/

-- [ Backup Tasks] -----------------------------------------------------------------------

-- [Post-Backup Tasks] -----------------------------------------------------------------------
-- Task 1: Create a Dev database and test the backup at the same time!
Use [Master]
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pRefreshDoctorsSchedulesDev')
  Begin
    Drop Proc pRefreshDoctorsSchedulesDev;
  End
go

Create -- Drop
Proc pRefreshDoctorsSchedulesDev
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Creates/Refreshes a Dev Database based on the Sunday "First File" 
--       backup of the DoctorsSchedules DB. 
--       (If you wanted the latest file each day you need to add logic to find it with Restore_Header)
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Restore Code --
    -- Step 1) Kick everyone off the database as needed 
    If Exists (Select * From Sys.databases where Name = 'DoctorsSchedulesDev')
      Begin
        Alter Database DoctorsSchedulesDev set Single_user with rollback immediate;
        --Drop Database DoctorsSchedulesDev
      End
    -- Step 2) Restore the DoctorsSchedules database as the DoctorsSchedulesDev database
    Restore database DoctorsSchedulesDev 
      From Disk = N'C:\BackupFiles\DoctorsSchedulesBackupDevice.bak' 
      With File = 1
          , Move N'DoctorsSchedules' TO N'C:\BackupFiles\DoctorsSchedulesDev.mdf'
          , Move N'DoctorsSchedules_log' TO N'C:\BackupFiles\DoctorsSchedulesDev.ldf'
          , Recovery -- Makes the DB open for use
          , Replace -- Replaces the DB as needed 
		-- Step 3) Change to Multi-User (should not need this, but I have seen it "Stick" to Single_User before) 
    Alter Database DoctorsSchedulesDev set Multi_user with rollback immediate;
    -- Restore Code --
		Print 'Success in Restoring the DoctorsSchedules database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Restoring the DoctorsSchedules database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DoctorsSchedulesBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pRefreshDoctorsSchedulesDev;
Select * From DoctorsSchedulesDev.Sys.Tables;
go  
****************************************************************************************************/

-- Task 2: Create a Spoc to test the restored database
Use [Master];
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pTestRestoreFromDoctorsSchedules')
  Begin
    Drop Proc [pTestRestoreFromDoctorsSchedules];
  End
go

Create Proc pTestRestoreFromDoctorsSchedules
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Test that the DoctorsSchedules and the DoctorsSchedulesDev are the same.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created File
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
    -- Setup Code -- 
      If NOT Exists (SELECT name FROM TempDB.sys.objects WHERE name = N'DoctorsSchedulesMaintLog')
        Begin
          Create Table TempDB.dbo.DoctorsSchedulesMaintLog
          ( LogID int Primary Key Identity
          , LogEntry nvarchar(2000) 
          , LogEntryDate datetime Default GetDate()
          );
        End
	  -- Validate Restore Code --
      Declare @CurrentCount int, @RestoredCount int;
      -- Test Row Counts
      Select @CurrentCount = count(*) From [DoctorsSchedules].[dbo].[DoctorShifts];
      Select @RestoredCount = count(*) From [DoctorsSchedulesDev].[dbo].[DoctorShifts];
      If (@CurrentCount = @RestoredCount)
        Insert Into TempDB.dbo.DoctorsSchedulesMaintLog (LogEntry)
         Select [Test] = 'Row Count Test: Passed';
      Else
        Insert Into TempDB.dbo.DoctorsSchedulesMaintLog (LogEntry)
         Select [Test] = 'Row Count Test: Failed';
      -- Review Data
      Select Top (5) * From [DoctorsSchedules].[dbo].[DoctorShifts] Order By 1 Desc;
      Select Top (5) * From  [DoctorsSchedulesDev].[dbo].[DoctorShifts] Order By 1 Desc;
		-- Validate Restore Code --
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Testing the DoctorsSchedules database Backup and Restore';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database DoctorsSchedules To DoctorsSchedulesBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DoctorsSchedulesBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pRefreshDoctorsSchedulesDev;
Select * From DoctorsSchedulesDev.Sys.Tables;
go  

-- Step 3) Test the stored procedure
Exec Master.dbo.pTestRestoreFromDoctorsSchedules;
go  
****************************************************************************************************/
Print 'Stored Procedures Created for Backing up DoctorSchedules! Make sure to test them!'

/* Test ALL the stored procedures **********************************************************************

-- Step 1) Create a Folder (as needed)
!! MD C:\BackupFiles

-- Step 2) Delete backup Device (as needed)
Exec master.dbo.sp_dropdevice @logicalname = N'DoctorsSchedulesBackupDevice';

-- Step 3) Test the Sprocs
Exec pCreateDoctorsSchedulesBackupDevice;
go  
Exec Master.dbo.pMaintBackupDoctorsSchedules;
go
Exec Master.dbo.pRefreshDoctorsSchedulesDev;
go  
Exec Master.dbo.pTestRestoreFromDoctorsSchedules;
go  
Select * From DoctorsSchedulesDev.Sys.Tables;
Select * From TempDB.dbo.DoctorsSchedulesMaintLog;
****************************************************************************************************/
--**************************************************************************--


--**************************************************************************--
--- Backup for Patients ---

--[ Pre-Backup Tasks]---------------------------------------------------------------------
-- Task 1: Create Backup Devices as needed --
If Exists (SELECT name FROM sys.objects WHERE name = N'pCreatePatientsBackupDevice')
Begin
  Drop Proc pCreatePatientsBackupDevice;
End
go

Create -- Drop
Proc pCreatePatientsBackupDevice
--*************************************************************************--
-- Dev: Wayne Wang
-- Desc: Creates a backup device for the Patients DB as needed.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Backup Device Code --
    If NOT Exists (Select * From model.sys.sysdevices Where Name = N'PatientsBackupDevice')
      Exec master.dbo.sp_AdDumpDevice
        @devtype = N'disk'
      , @logicalname = N'PatientsBackupDevice'
      , @physicalname = N'C:\BackupFiles\PatientsBackupDevice.bak'
		-- Backup Device Code --
		Print 'Success in Creating Backing Device for the Patients database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error in Creating Backing Device for the Patients database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) Create a Folder (as needed)
!! MD C:\BackupFiles

-- Step 2) Delete backup Device (as needed)
Exec master.dbo.sp_dropdevice @logicalname = N'PatientsBackupDevice';

-- Step 3) Test the Sproc
Exec pCreatePatientsBackupDevice;
go  
****************************************************************************************************/

--[ Pre-Backup Tasks]---------------------------------------------------------------------

-- [ Backup Tasks] -----------------------------------------------------------------------
-- Task 1: Backup the Patients Database
Use [Master];
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pMaintBackupPatients')
Begin
  Drop Proc pMaintBackupPatients;
End
go

Create -- Drop
Proc pMaintBackupPatients
--*************************************************************************--
-- Dev: Wayne Wang
-- Desc: Performs database backups on the Patients DB.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Backup Code --
    If (DatePart(dw,GetDate()) = 1)-- Sunday
      Backup Database Patients To PatientsBackupDevice With Name = 'Sun-Full', Init;
    Else                                                             
    If (DatePart(dw,GetDate()) = 2)-- Monday                         
      Backup Database Patients To PatientsBackupDevice With Name = 'Mon-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 3)-- Tuesday                        
      Backup Database Patients To PatientsBackupDevice With Name = 'Tue-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 4)-- Wednesday                      
      Backup Database Patients To PatientsBackupDevice With Name = 'Wed-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 5)-- Thursday                       
      Backup Database Patients To PatientsBackupDevice With Name = 'Thu-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 6)-- Friday                         
      Backup Database Patients To PatientsBackupDevice With Name = 'Fri-Full';  
    Else                                                             
    If (DatePart(dw,GetDate()) = 7)-- Saturday                       
      Backup Database Patients To PatientsBackupDevice With Name = 'Sat-Full';
		-- Backup Code --
		Print 'Success in Backing up the Patients database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Backing up the Patients database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database Patients To PatientsBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\PatientsBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pMaintBackupPatients;
Restore HeaderOnly From DISK = N'C:\BackupFiles\PatientsBackupDevice.bak';
go  
****************************************************************************************************/

-- [ Backup Tasks] -----------------------------------------------------------------------

-- [Post-Backup Tasks] -----------------------------------------------------------------------
-- Task 1: Create a Dev database and test the backup at the same time!
Use [Master]
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pRefreshPatientsDev')
  Begin
    Drop Proc pRefreshPatientsDev;
  End
go

Create -- Drop
Proc pRefreshPatientsDev
--*************************************************************************--
-- Dev: Wayne Wang
-- Desc: Creates/Refreshes a Dev Database based on the Sunday "First File" 
--       backup of the Patients DB. 
--       (If you wanted the latest file each day you need to add logic to find it with Restore_Header)
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Restore Code --
    -- Step 1) Kick everyone off the database as needed 
    If Exists (Select * From Sys.databases where Name = 'PatientsDev')
      Begin
        Alter Database PatientsDev set Single_user with rollback immediate;
      End
    -- Step 2) Restore the Patients database as the DoctorsSchedulesDev database
    Restore database PatientsDev 
      From Disk = N'C:\BackupFiles\PatientsBackupDevice.bak' 
      With File = 1
          , Move N'Patients' TO N'C:\BackupFiles\PatientsDev.mdf'
          , Move N'Patients_log' TO N'C:\BackupFiles\PatientsDev.ldf'
          , Recovery -- Makes the DB open for use
          , Replace -- Replaces the DB as needed 
		-- Step 3) Change to Multi-User (should not need this, but I have seen it "Stick" to Single_User before) 
    Alter Database PatientsDev set Multi_user with rollback immediate;
    -- Restore Code --
		Print 'Success in Restoring the Patients database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Restoring the Patients database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database Patients To PatientsBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\PatientsBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pRefreshPatientsDev;
Select * From PatientsDev.Sys.Tables;
go  
****************************************************************************************************/

-- Task 2: Create a Spoc to test the restored database
Use [Master];
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pTestRestoreFromPatients')
  Begin
    Drop Proc [pTestRestoreFromPatients];
  End
go

Create Proc pTestRestoreFromPatients
--*************************************************************************--
-- Dev: Wayne Wang
-- Desc: Test that the Patients and the PatientsDev are the same.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created File
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
    -- Setup Code -- 
      If NOT Exists (SELECT name FROM TempDB.sys.objects WHERE name = N'PatientsMaintLog')
        Begin
          Create Table TempDB.dbo.PatientsMaintLog
          ( LogID int Primary Key Identity
          , LogEntry nvarchar(2000) 
          , LogEntryDate datetime Default GetDate()
          );
        End
	  -- Validate Restore Code --
      Declare @CurrentCount int, @RestoredCount int;
      -- Test Row Counts
      Select @CurrentCount = count(*) From [Patients].[dbo].[Visits];
      Select @RestoredCount = count(*) From [PatientsDev].[dbo].[Visits];
      If (@CurrentCount = @RestoredCount)
        Insert Into TempDB.dbo.PatientsMaintLog (LogEntry)
         Select [Test] = 'Row Count Test: Passed';
      Else
        Insert Into TempDB.dbo.PatientsMaintLog (LogEntry)
         Select [Test] = 'Row Count Test: Failed';
      -- Review Data
      Select Top (5) * From [Patients].[dbo].[Visits] Order By 1 Desc;
      Select Top (5) * From [PatientsDev].[dbo].[Visits] Order By 1 Desc;
		-- Validate Restore Code --
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Testing the Patients database Backup and Restore';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database Patients To PatientsBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\PatientsBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pRefreshPatientsDev;
Select * From PatientsDev.Sys.Tables;
go  

-- Step 3) Test the stored procedure
Exec Master.dbo.pTestRestoreFromPatients;
go  
****************************************************************************************************/
Print 'Stored Procedures Created for Backing up Patients! Make sure to test them!'

/* Test ALL the stored procedures **********************************************************************

-- Step 1) Create a Folder (as needed)
!! MD C:\BackupFiles

-- Step 2) Delete backup Device (as needed)
Exec master.dbo.sp_dropdevice @logicalname = N'PatientsBackupDevice';

-- Step 3) Test the Sprocs
Exec pCreatePatientsBackupDevice;
go  
Exec Master.dbo.pMaintBackupPatients;
go
Exec Master.dbo.pRefreshPatientsDev;
go  
Exec Master.dbo.pTestRestoreFromPatients;
go  
Select * From PatientsDev.Sys.Tables;
Select * From TempDB.dbo.PatientsMaintLog;
****************************************************************************************************/
--**************************************************************************--



--**************************************************************************--
--- Backup for DWClinicReportData ---

--[ Pre-Backup Tasks]---------------------------------------------------------------------
-- Task 1: Create Backup Devices as needed --
If Exists (SELECT name FROM sys.objects WHERE name = N'pCreateDWClinicReportDataBackupDevice')
Begin
  Drop Proc pCreateDWClinicReportDataBackupDevice;
End
go

Create -- Drop
Proc pCreateDWClinicReportDataBackupDevice
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Creates a backup device for the DWClinicReportData DB as needed.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Backup Device Code --
    If NOT Exists (Select * From model.sys.sysdevices Where Name = N'DWClinicReportDataBackupDevice')
      Exec master.dbo.sp_AdDumpDevice
        @devtype = N'disk'
      , @logicalname = N'DWClinicReportDataBackupDevice'
      , @physicalname = N'C:\BackupFiles\DWClinicReportDataBackupDevice.bak'
		-- Backup Device Code --
		Print 'Success in Creating Backing Device for the DWClinicReportData database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error in Creating Backing Device for the DWClinicReportData database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) Create a Folder (as needed)
!! MD C:\BackupFiles

-- Step 2) Delete backup Device (as needed)
Exec master.dbo.sp_dropdevice @logicalname = N'DWClinicReportDataBackupDevice';

-- Step 3) Test the Sproc
Exec pCreateDWClinicReportDataBackupDevice;
go  
****************************************************************************************************/



--[ Pre-Backup Tasks]---------------------------------------------------------------------

-- [ Backup Tasks] -----------------------------------------------------------------------
-- Task 1: Backup the DWClinicReportData Database
Use [Master];
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pMaintBackupDWClinicReportData')
Begin
  Drop Proc pMaintBackupDWClinicReportData;
End
go

Create -- Drop
Proc pMaintBackupDWClinicReportData
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Performs database backups on the DWClinicReportData DB.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Backup Code --
    If (DatePart(dw,GetDate()) = 1)-- Sunday
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Sun-Full', Init;
    Else                                                             
    If (DatePart(dw,GetDate()) = 2)-- Monday                         
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Mon-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 3)-- Tuesday                        
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Tue-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 4)-- Wednesday                      
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Wed-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 5)-- Thursday                       
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Thu-Full';
    Else                                                             
    If (DatePart(dw,GetDate()) = 6)-- Friday                         
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Fri-Full';  
    Else                                                             
    If (DatePart(dw,GetDate()) = 7)-- Saturday                       
      Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Sat-Full';
		-- Backup Code --
		Print 'Success in Backing up the DWClinicReportData database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Backing up the DWClinicReportData database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DWClinicReportDataBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pMaintBackupDWClinicReportData;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DWClinicReportDataBackupDevice.bak';
go  
****************************************************************************************************/

-- [ Backup Tasks] -----------------------------------------------------------------------

-- [Post-Backup Tasks] -----------------------------------------------------------------------
-- Task 1: Create a Dev database and test the backup at the same time!
Use [Master]
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pRefreshDWClinicReportDataDev')
  Begin
    Drop Proc pRefreshDWClinicReportDataDev;
  End
go

Create -- Drop
Proc pRefreshDWClinicReportDataDev
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Creates/Refreshes a Dev Database based on the Sunday "First File" 
--       backup of the DWClinicReportData DB. 
--       (If you wanted the latest file each day you need to add logic to find it with Restore_Header)
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created Sproc
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
		-- Restore Code --
    -- Step 1) Kick everyone off the database as needed 
    If Exists (Select * From Sys.databases where Name = 'DWClinicReportDataDev')
      Begin
        Alter Database DWClinicReportDataDev set Single_user with rollback immediate;
        --Drop Database DWClinicReportDataDev
      End
    -- Step 2) Restore the DWClinicReportData database as the DWClinicReportDataDev database
    Restore database DWClinicReportDataDev 
      From Disk = N'C:\BackupFiles\DWClinicReportDataBackupDevice.bak' 
      With File = 1
          , Move N'DWClinicReportData' TO N'C:\BackupFiles\DWClinicReportDataDev.mdf'
          , Move N'DWClinicReportData_log' TO N'C:\BackupFiles\DWClinicReportDataDev.ldf'
          , Recovery -- Makes the DB open for use
          , Replace -- Replaces the DB as needed 
		-- Step 3) Change to Multi-User (should not need this, but I have seen it "Stick" to Single_User before) 
    Alter Database DWClinicReportDataDev set Multi_user with rollback immediate;
    -- Restore Code --
		Print 'Success in Restoring the DWClinicReportData database';
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Restoring the DWClinicReportData database';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DWClinicReportDataBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pRefreshDWClinicReportDataDev;
Select * From DWClinicReportDataDev.Sys.Tables;
go  
****************************************************************************************************/

-- Task 2: Create a Spoc to test the restored database
Use [Master];
go

If Exists (SELECT name FROM sys.objects WHERE name = N'pTestRestoreFromDWClinicReportData')
  Begin
    Drop Proc [pTestRestoreFromDWClinicReportData];
  End
go

Create Proc pTestRestoreFromDWClinicReportData
--*************************************************************************--
-- Dev:  Wayne Wang
-- Desc: Test that the DWClinicReportData and the DWClinicReportDataDev are the same.
-- Change Log: When,Who,What
-- 2019-08-26, Wayne Wang,Created File
--**************************************************************************--
As 
Begin
	Declare @RC int = 0;
	Begin Try 
    -- Setup Code -- 
      If NOT Exists (SELECT name FROM TempDB.sys.objects WHERE name = N'DWClinicReportDataMaintLog')
        Begin
          Create Table TempDB.dbo.DWClinicReportDataMaintLog
          ( LogID int Primary Key Identity
          , LogEntry nvarchar(2000) 
          , LogEntryDate datetime Default GetDate()
          );
        End
	  -- Validate Restore Code --
      Declare @CurrentCount int, @RestoredCount int;
      -- Test Row Counts
      Select @CurrentCount = count(*) From [DWClinicReportData].[dbo].[FactVisits];
      Select @RestoredCount = count(*) From [DWClinicReportDataDev].[dbo].[FactVisits];
      If (@CurrentCount = @RestoredCount)
        Insert Into TempDB.dbo.DWClinicReportDataMaintLog (LogEntry)
         Select [Test] = 'Row Count Test: Passed';
      Else
        Insert Into TempDB.dbo.DWClinicReportDataMaintLog (LogEntry)
         Select [Test] = 'Row Count Test: Failed';
      -- Review Data
      Select Top (5) * From [DWClinicReportData].[dbo].[FactVisits] Order By 1 Desc;
      Select Top (5) * From  [DWClinicReportDataDev].[dbo].[FactVisits] Order By 1 Desc;
		-- Validate Restore Code --
		Set @RC = 1;
  End Try
  Begin Catch 
		Print 'Error Testing the DWClinicReportData database Backup and Restore';
		Print ERROR_MESSAGE();
		Set @RC = -1;
  End Catch
  Return @RC;
End -- Proc
go

/* Test the stored procedure **********************************************************************
-- Step 1) clear out the backup file and add a Ad-Hoc backup
Backup Database DWClinicReportData To DWClinicReportDataBackupDevice With Name = 'Ad-Hoc-Full', Init;
Restore HeaderOnly From DISK = N'C:\BackupFiles\DWClinicReportDataBackupDevice.bak';
go 

-- Step 2) Test the stored procedure
Exec Master.dbo.pRefreshDWClinicReportDataDev;
Select * From DWClinicReportDataDev.Sys.Tables;
go  

-- Step 3) Test the stored procedure
Exec Master.dbo.pTestRestoreFromDWClinicReportData;
go  
****************************************************************************************************/
Print 'Stored Procedures Created for Backing up DWClinicReportData! Make sure to test them!'

/* Test ALL the stored procedures **********************************************************************

-- Step 1) Create a Folder (as needed)
!! MD C:\BackupFiles

-- Step 2) Delete backup Device (as needed)
Exec master.dbo.sp_dropdevice @logicalname = N'DWClinicReportDataBackupDevice';

-- Step 3) Test the Sprocs
Exec pCreateDWClinicReportDataBackupDevice;
go  
Exec Master.dbo.pMaintBackupDWClinicReportData;
go
Exec Master.dbo.pRefreshDWClinicReportDataDev;
go  
Exec Master.dbo.pTestRestoreFromDWClinicReportData;
go  
Select * From DWClinicReportDataDev.Sys.Tables;
Select * From TempDB.dbo.DWClinicReportDataMaintLog;
****************************************************************************************************/