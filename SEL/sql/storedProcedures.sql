/* Check if a Unit exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_UNIT(@unitID as INT, @unitName as NVARCHAR(50), @unitGUID as UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT unitGUID
		from dbresprod.dbo.SEL_UNITS
		WHERE unitID = @unitID
	)
	BEGIN
		SELECT @unitGUID = unitGUID
		from dbresprod.dbo.SEL_UNITS
		WHERE unitID = @unitID
	END

	ELSE
		BEGIN
			SET @unitGUID = NULL
			SET @unitGUID = NEWID()
			INSERT INTO dbresprod.dbo.SEL_UNITS (unitGUID, unitID, unitName) VALUES (@unitGUID, @unitID, @unitName)
		END
END
GO

/* Check if a Mode exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_MODE (@modeID as INT, @modeGUID as UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT modeGUID
		from dbresprod.dbo.SEL_MODES
		WHERE modeID = @modeID
	)
	BEGIN
		SELECT @modeGUID = modeGUID
		from dbresprod.dbo.SEL_MODES
		WHERE modeID = @modeID
	END

	ELSE
		BEGIN
			SET @modeGUID = NULL
			SET @modeGUID = NEWID()
			INSERT INTO dbresprod.dbo.SEL_MODES (modeGUID, modeID) VALUES (@unitGUID, @unitID)
		END
END
GO

/* Check if a Status exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_STATUS (@statusID as INT, @statusGUID as UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT statusGUID
		from dbresprod.dbo.SEL_STATUSES
		WHERE statusID = @statusID
	)
	BEGIN
		SELECT @statusGUID = statusGUID
		from dbresprod.dbo.SEL_MODES
		WHERE modeID = @modeID
	END

	ELSE
		BEGIN
			SET @statusGUID = NULL
			SET @statusGUID = NEWID()
			INSERT INTO dbresprod.dbo.SEL_STATUSES (statusGUID, statusID, statusName) VALUES (@statusGUID, @statusID, @statusName)
		END
END
GO

/* Check if a Update exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_UPDATE (@lastUpdate as DATETIME, @updateGUID as UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @updateGUID = NULL
	SET @updateGUID = NEWID()
	INSERT INTO dbresprod.dbo.SEL_UPDATES (updateGUID, lastUpdate) VALUES (@updateGUID, @lastUpdate)
END
GO

/* Check if a Type exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_TYPE (@typeID as INT, @typeGUID as UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT typeGUID
		from dbresprod.dbo.SEL_TYPES
		WHERE typeID = @typeID
	)
	BEGIN
		SELECT @typeGUID = typeGUID
		FROM dbresprod.dbo.SEL_TYPES
		WHERE typeID = @typeID
	END

	ELSE
		BEGIN
			SET @typeGUID = NULL
			SET @typeGUID = NEWID()
			INSERT INTO dbresprod.dbo.SEL_TYPES (typeGUID, typeID) VALUES (@typeGUID, @typeID)
		END
END
GO

/* Check if a Measurement Unit exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_GET_OR_CREATE_SEL_MEASURE_UNIT (@mUnitName as NVARCHAR(20), @mUnitGUID as UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT mUnitGUID
		FROM dbresprod.dbo.SEL_MEASURE_UNITS
		WHERE mUnitName = @mUnitName
	)
	BEGIN
		SELECT @mUnitGUID = mUnitGUID
		FROM dbresprod.sbo.SEL_MEASURE_UNITS
		WHERE mUnitGUID = @mUnitGUID
	END

	ELSE
		BEGIN
			SET @mUnitGUID = NULL
			SET @mUnitGUID = NEWID()
			INSERT INTO dbresprod.sbo.SEL_MEASURE_UNITS (mUnitGUID, munitName) VALUES (@munitGUID, @munitName)
		END
END
GO

/* Check if an Output exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_OUTPUT (@outputGUID as UNIQUEIDENTIFIER OUTPUT, @unitGUID as UNIQUEIDENTIFIER, @updateGUID as UNIQUEIDENTIFIER, @modeGUID as UNIQUEIDENTIFIER, @statusGUID as UNIQUEIDENTIFIER, @outputID as INT, @outputName as NVARCHAR(50), @highstate as NVARCHAR(5), @lowstate as NVARCHAR(5))
AS
BEGIN
	SET NOCOUNT ON;
	SET @outputGUID = NULL
	SET @outputGUID = NEWID()
	INSERT INTO dbresprod.dbo.SEL_OUTPUTS (outputGUID, mUnitGUID, updateGUID, modeGUID, statusGUID, ouputID, outputName, highState, lowState) VALUES (@outputGUID, @mUnitGUID, @updateGUID, @modeGUID, @statusGUID, @ouputID, @outputName, @highState, @lowState)
END
GO

/* Check if an Request exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_REQUEST (@requestGUID as UNIQUEIDENTIFIER OUTPUT, @unitGUID as UNIQUEIDENTIFIER, @updateGUID as UNIQUEIDENTIFIER, @modeGUID as UNIQUEIDENTIFIER, @success as BIT, @requestMessage as NVARCHAR(35), @requestNow as DATETIME, @requestName as NVARCHAR(50), @tz as NVARCHAR(25), @updateCycle as INT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @requestGUID = NULL
	SET @requestGUID = NEWID()
	INSERT INTO dbresprod.dbo.SEL_REQUESTS (requestGUID, unitGUID, updateGUID, modeGUID, success, requestMessage, requestNow, requestName, tz, updateCycle) VALUES (@requestGUID, @unitGUID, @updateGUID, @modeGUID, @success, @requestMessage, @requestNow, @requestName, @tz, @updateCycle)
END
GO

/* Check if an Alarm exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_ALARM (@alarmGUID as UNIQUEIDENTIFIER OUTPUT, @unitGUID as UNIQUEIDENTIFIER, @typeGUID as UNIQUEIDENTIFIER, @statusGUID as UNIQUEIDENTIFIER, @mUnitGUID as UNIQUEIDENTIFIER, @alarmID as INT, @alarmName as NVARCHAR(50), @lastChange as DATETIME, @healthyName as NVARCHAR(10), @faultyName as NVARCHAR(10), @pulseTotal as FLOAT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @alarmGUID = NULL
	SET @alarmGUID = NEWID()
	INSERT INTO dbresprod.dbo.SEL_ALARMS (alarmGUID, unitGUID, typeGUID, statusGUID, mUnitGUID, alarmID, alarmName, lastChange, healthyName, faultyName, pulsetotal) VALUES (@alarmGUID, @unitGUID, @typeGUID, @statusGUID, @mUnitGUID, @alarmID, @alarmName, @lastChange, @healthyName, @faultyName, @pulsetotal)
END
GO

/* Check if an Reading exists, create if it doesnt, or select GUID if it does */
CREATE PROCEDURE PROC_CREATE_SEL_READING (@readingGUID as UNIQUEIDENTIFIER OUTPUT, @unitGUID as UNIQUEIDENTIFIER, @mUnitGUID as UNIQUEIDENTIFIER, @analogID as INT, @readingName as NVARCHAR(50), @readingValue as NVARCHAR(10), @recharge as INT, @cyclePulses as FLOAT, @readingStart as INT, @readingStop as INT, @dp as INT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @readingGUID = NULL
	SET @readingGUID = NEWID()
	INSERT INTO dbresprod.dbo.SEL_READINGS (readingGUID, unitGUID, mUnitGUID, analogID, readingName, readingValue, recharge, cyclePulses, readingStart, readingStop, dp) VALUES (@readingGUID, @unitGUID, @mUnitGUID, @analogID, @readingName, @readingValue, @recharge, @cyclePulses, @readingStart, @readingStop, @dp)
END
GO