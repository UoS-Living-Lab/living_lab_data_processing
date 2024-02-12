USE [dbresprod]
GO

/* Check if a Unit exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_UNIT(@unitID AS INT, @unitName AS NVARCHAR(50), @unitGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT unitGUID
		from dbo.SEL_UNITS
		WHERE unitID = @unitID
	)
	BEGIN
		SELECT @unitGUID = unitGUID
		from dbo.SEL_UNITS
		WHERE unitID = @unitID
	END

	ELSE
		BEGIN
			SET @unitGUID = NULL
			SET @unitGUID = NEWID()
			INSERT INTO dbo.SEL_UNITS (unitGUID, unitID, unitName) VALUES (@unitGUID, @unitID, @unitName)
		END
END
GO


/* Check if a Mode exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_MODE (@modeID AS INT, @modeGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT modeGUID
		from dbo.SEL_MODES
		WHERE modeID = @modeID
	)
	BEGIN
		SELECT @modeGUID = modeGUID
		from dbo.SEL_MODES
		WHERE modeID = @modeID
	END

	ELSE
		BEGIN
			SET @modeGUID = NULL
			SET @modeGUID = NEWID()
			INSERT INTO dbo.SEL_MODES (modeGUID, modeID) VALUES (@modeGUID, @modeID)
		END
END
GO


/* Check if a Status exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_STATUS (@statusID AS INT, @statusGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT statusGUID
		from dbo.SEL_STATUSES
		WHERE statusID = @statusID
	)
	BEGIN
		SELECT @statusGUID = statusGUID
		from dbo.SEL_STATUSES
		WHERE statusID = @statusID
	END

	ELSE
		BEGIN
			SET @statusGUID = NULL
			SET @statusGUID = NEWID()
			INSERT INTO dbo.SEL_STATUSES (statusGUID, statusID) VALUES (@statusGUID, @statusID)
		END
END
GO


/* Check if a Type exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_SENSOR (@sensorName AS NVARCHAR(20), @sensorGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT sensorGUID
		from dbo.SEL_SENSORS
		WHERE sensorName = @sensorName
	)
	BEGIN
		SELECT @sensorGUID = sensorGUID
		FROM dbo.SEL_SENSORS
		WHERE sensorName = @sensorName
	END

	ELSE
		BEGIN
			SET @sensorGUID = NULL
			SET @sensorGUID = NEWID()
			INSERT INTO dbo.SEL_SENSORS (sensorGUID, sensorName) VALUES (@sensorGUID, @sensorName)
		END
END
GO


/* Check if a Type exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_TYPE (@typeID AS INT, @typeGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT typeGUID
		from dbo.SEL_TYPES
		WHERE typeID = @typeID
	)
	BEGIN
		SELECT @typeGUID = typeGUID
		FROM dbo.SEL_TYPES
		WHERE typeID = @typeID
	END

	ELSE
		BEGIN
			SET @typeGUID = NULL
			SET @typeGUID = NEWID()
			INSERT INTO dbo.SEL_TYPES (typeGUID, typeID) VALUES (@typeGUID, @typeID)
		END
END
GO


/* Check if a Measurement Unit exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_MEASURE_UNIT (@mUnitName AS NVARCHAR(20), @mUnitGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT mUnitGUID
		FROM dbo.SEL_MEASURE_UNITS
		WHERE mUnitName = @mUnitName
	)
	BEGIN
		SELECT @mUnitGUID = mUnitGUID
		FROM dbo.SEL_MEASURE_UNITS
		WHERE mUnitName = @mUnitName
	END

	ELSE
		BEGIN
			SET @mUnitGUID = NULL
			SET @mUnitGUID = NEWID()
			INSERT INTO dbo.SEL_MEASURE_UNITS (mUnitGUID, mUnitName) VALUES (@munitGUID, @munitName)
		END
END
GO


/* Check if an Output exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_OUTPUT (@unitGUID AS UNIQUEIDENTIFIER, @modeGUID AS UNIQUEIDENTIFIER, @statusGUID AS UNIQUEIDENTIFIER, @outputID AS INT, @outputName AS NVARCHAR(50), @highstate AS NVARCHAR(5), @lowstate AS NVARCHAR(5), @outputGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @outputGUID = NULL
	SET @outputGUID = NEWID()
	INSERT INTO dbo.SEL_OUTPUTS (outputGUID, unitGUID, modeGUID, statusGUID, outputID, outputName, highState, lowState) VALUES (@outputGUID, @unitGUID, @modeGUID, @statusGUID, @outputID, @outputName, @highState, @lowState)
END
GO


/* Check if an Request exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_REQUEST (@unitGUID AS UNIQUEIDENTIFIER, @modeGUID AS UNIQUEIDENTIFIER, @success AS BIT, @requestMessage AS NVARCHAR(35), @requestNow AS DATETIME, @requestName AS NVARCHAR(50), @tz AS NVARCHAR(25), @updateCycle AS INT, @requestGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @requestGUID = NULL
	SET @requestGUID = NEWID()
	INSERT INTO dbo.SEL_REQUESTS (requestGUID, unitGUID, modeGUID, success, requestMessage, requestNow, requestName, tz, updateCycle) VALUES (@requestGUID, @unitGUID, @modeGUID, @success, @requestMessage, @requestNow, @requestName, @tz, @updateCycle)
END
GO


/* Check if an Alarm exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_ALARM (@unitGUID AS UNIQUEIDENTIFIER, @typeGUID AS UNIQUEIDENTIFIER, @statusGUID AS UNIQUEIDENTIFIER, @mUnitGUID AS UNIQUEIDENTIFIER, @alarmID AS INT, @alarmName AS NVARCHAR(50), @healthyName AS NVARCHAR(10), @faultyName AS NVARCHAR(10), @pulseTotal AS FLOAT, @alarmGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @alarmGUID = NULL
	SET @alarmGUID = NEWID()
	INSERT INTO dbo.SEL_ALARMS (alarmGUID, unitGUID, typeGUID, statusGUID, mUnitGUID, alarmID, alarmName, healthyName, faultyName, pulsetotal) VALUES (@alarmGUID, @unitGUID, @typeGUID, @statusGUID, @mUnitGUID, @alarmID, @alarmName, @healthyName, @faultyName, @pulsetotal)
END
GO


/* Check if an Reading exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_READING (@unitGUID AS UNIQUEIDENTIFIER, @mUnitGUID AS UNIQUEIDENTIFIER, @sensorGUID AS UNIQUEIDENTIFIER, @analogID AS INT, @readingValue AS NVARCHAR(10), @recharge AS INT, @cyclePulses AS FLOAT, @readingStart AS FLOAT, @readingStop AS FLOAT, @dp AS INT, @readingGUID AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @readingGUID = NULL
	SET @readingGUID = NEWID()
	INSERT INTO dbo.SEL_READINGS (readingGUID, unitGUID, mUnitGUID, sensorGUID, analogID, readingValue, recharge, cyclePulses, readingStart, readingStop, dp) VALUES (@readingGUID, @unitGUID, @mUnitGUID, @sensorGUID, @analogID, @readingValue, @recharge, @cyclePulses, @readingStart, @readingStop, @dp)
END
GO


/* Check if a Update exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_UPDATE (@requestGUID AS UNIQUEIDENTIFIER, @alarmGUID AS UNIQUEIDENTIFIER, @readingGUID AS UNIQUEIDENTIFIER, @outputGUID AS UNIQUEIDENTIFIER, @lastUpdate AS DATETIME)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO dbo.SEL_UPDATES (requestGUID, alarmGUID, readingGUID, outputGUID, lastUpdate) VALUES (@requestGUID, @alarmGUID, @readingGUID, @outputGUID, @lastUpdate)
END
GO