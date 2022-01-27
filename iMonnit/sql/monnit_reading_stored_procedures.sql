/* Retrieve all data and return the result */
CREATE PROCEDURE PROC_GET_ALL_MONNIT_DATA
AS
BEGIN
	SET NOCOUNT ON;

	SELECT s.[sensorName]
		,r.[messageDate]
		,r.[rawData]
		,r.[dataValue]
		,dt.[dataType]
		,r.[plotValue]
		,pl.[plotLabel]
	FROM [salfordMove].[dbo].[READINGS] AS r
	JOIN [salfordMOVE].[dbo].SENSORS AS s
		ON (r.sensorID = s.sensorID)
	JOIN [salfordMOVE].[dbo].PLOT_LABELS as pl
		ON (r.plotLabelID = pl.plotLabelID)
	JOIN [salfordMOVE].[dbo].DATA_TYPES as dt
		ON (r.dataTypeID = dt.dataTypeID)

	ORDER BY messageDate DESC
END
GO


CREATE PROCEDURE PROC_GET_MONNIT_DATA(@start_date AS DATETIME, @end_date AS DATETIME)
AS
BEGIN
	SET NOCOUNT ON;

	SELECT s.[sensorName]
		,r.[messageDate]
		,r.[rawData]
		,r.[dataValue]
		,dt.[dataType]
		,r.[plotValue]
		,pl.[plotLabel]
	FROM [salfordMove].[dbo].[READINGS] AS r
	JOIN [salfordMOVE].[dbo].SENSORS AS s
		ON (r.sensorID = s.sensorID)
	JOIN [salfordMOVE].[dbo].PLOT_LABELS as pl
		ON (r.plotLabelID = pl.plotLabelID)
	JOIN [salfordMOVE].[dbo].DATA_TYPES as dt
		ON (r.dataTypeID = dt.dataTypeID)
	WHERE up.[lastUpdate] < @start_date AND up.[lastUpdate] > @end_date

	ORDER BY messageDate DESC
END
GO


CREATE PROCEDURE PROC_GET_ALL_MONNIT_READINGS
AS
BEGIN
	SET NOCOUNT ON;

	SELECT s.[sensorName]
		,r.[messageDate]
		,r.[rawData]
		,r.[dataValue]
		,dt.[dataType]
		,r.[plotValue]
		,pl.[plotLabel]
	FROM [salfordMove].[dbo].[READINGS] AS r
	JOIN [salfordMOVE].[dbo].SENSORS AS s
		ON (r.sensorID = s.sensorID)
	JOIN [salfordMOVE].[dbo].PLOT_LABELS as pl
		ON (r.plotLabelID = pl.plotLabelID)
	JOIN [salfordMOVE].[dbo].DATA_TYPES as dt
		ON (r.dataTypeID = dt.dataTypeID)

	ORDER BY messageDate DESC
END
GO


CREATE PROCEDURE PROC_GET_MONNIT_READINGS(@start_date AS DATETIME, @end_date AS DATETIME)
AS
BEGIN
	SET NOCOUNT ON;

	SELECT s.[sensorName]
      ,r.[messageDate]
      ,r.[rawData]
      ,r.[dataValue]
	  ,dt.[dataType]
      ,r.[plotValue]
      ,pl.[plotLabel]
	FROM [dbo].[MONNIT_READINGS] AS r
	JOIN [dbo].MONNIT_SENSORS AS s
		ON (r.sensorID = s.sensorID)
	JOIN [dbo].MONNIT_PLOT_LABELS as pl
		ON (r.plotLabelID = pl.plotLabelID)
	JOIN [dbo].MONNIT_DATA_TYPES as dt
		ON (r.dataTypeID = dt.dataTypeID)
	WHERE up.[lastUpdate] < @start_date AND up.[lastUpdate] > @end_date
	ORDER BY messageDate DESC
END
GO