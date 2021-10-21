/* Check for an application entry and return the GUID if it exists, or create a new entry if none exists. */
CREATE PROCEDURE PROC_GET_OR_CREATE_TTN_APPLICATION (@application_name as NVARCHAR(35), @application_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT application_guid
		from dbresprod.dbo.TTN_APPLICATIONS
		WHERE application_name = @application_name
	)
	BEGIN
		SELECT application_guid
		from dbresprod.dbo.TTN_APPLICATIONS
		WHERE application_name = @application_name
	END
	ELSE
		BEGIN
			SET @application_guid = NULL
			SET @application_guid = NEWID()
			INSERT INTO dbresprod.dbo.TTN_APPLICATIONS (application_guid, application_name) VALUES (@application_guid, @application_name)
		END
END
GO


/* Creates a warning entry */
CREATE PROCEDURE PROC_CREATE_TTN_TTN_WARNING (@decoded_payload_warning AS NVARCHAR(MAX), @warning_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT warning_guid
		from dbresprod.dbo.TTN_WARNINGS
		WHERE decoded_payload_warning = @decoded_payload_warning
	)
	BEGIN
		SELECT warning_guid
		from dbresprod.dbo.TTN_WARNINGS
		WHERE decoded_payload_warning = @decoded_payload_warning
	END
	ELSE
		BEGIN
			SET @warning_guid = NULL
			SET @warning_guid = NEWID()
			INSERT INTO dbresprod.dbo.TTN_WARNINGS (warning_guid, decoded_payload_warning) VALUES (@warning_guid, @decoded_payload_warning)
		END
END
GO


/* Check for a gateway and return the GUID if it exists, or create a new entry if none exists. */
CREATE PROCEDURE PROC_GET_OR_CREATE_TTN_GATEWAY (@location_guid AS UNIQUEIDENTIFIER, @gateway_name AS NVARCHAR(30), @gateway_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT gateway_guid
		from dbresprod.dbo.TTN_GATEWAYS
		WHERE gateway_name = @gateway_name
	)
	BEGIN
		SELECT gateway_guid
		from dbresprod.dbo.TTN_GATEWAYS
		WHERE gateway_name = @gateway_name
	END
	ELSE
		BEGIN
			SET @gateway_guid = NULL
			SET @gateway_guid = NEWID()
			INSERT INTO dbresprod.dbo.TTN_GATEWAYS (gateway_guid, location_guid, gateway_name) VALUES (@gateway_guid, @location_guid, @gateway_name)
		END
END
GO


/* Check for a location entry and return the GUID if it exists, or create a new entry if none exists. */
CREATE PROCEDURE PROC_GET_OR_CREATE_TTN_LOCATION (@latitude as FLOAT, @longitude AS FLOAT, @source AS NVARCHAR(100), @location_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT location_guid
		from dbresprod.dbo.TTN_LOCATIONS
		WHERE latitude = @latitude AND longitude = @longitude AND source = @source
	)
	BEGIN
		SELECT location_guid
		from dbresprod.dbo.TTN_LOCATIONS
		WHERE latitude = @latitude AND longitude = @longitude AND source = @source
	END
	ELSE
		BEGIN
			SET @location_guid = NULL
			SET @location_guid = NEWID()
			INSERT INTO dbresprod.dbo.TTN_LOCATIONS (location_guid, latitude, longitude, source) VALUES (@location_guid, @latitude, @longitude, @source)
		END
END
GO


/* Check for a sensor and return the GUID if it exists, or create a new entry if none exists. */
CREATE PROCEDURE PROC_GET_OR_CREATE_TTN_SENSOR (@sensor_name AS NVARCHAR(30), @sensor_type AS NVARCHAR, @sensor_location AS NVARCHAR(30), @measurement_unit AS NVARCHAR(5), @sensor_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT sensor_guid
		from dbresprod.dbo.TTN_SENSORS
		WHERE sensor_name = @sensor_name
	)
	BEGIN
		SELECT sensor_guid
		from dbresprod.dbo.TTN_SENSORS
		WHERE sensor_name = @sensor_name
	END
	ELSE
		BEGIN
			SET @sensor_guid = NULL
			SET @sensor_guid = NEWID()
			INSERT INTO dbresprod.dbo.TTN_SENSORS (sensor_guid, sensor_name, sensor_type, sensor_location, measurement_unit, sensor_guid) VALUES (@sensor_guid, @sensor_name, @sensor_type, @sensor_location, @measurement_unit, @sensor_guid)
		END
END
GO


/* Check for a device and return the GUID if it exists, or create a new entry if none exists. */
CREATE PROCEDURE PROC_GET_OR_CREATE_TTN_DEVICE (@application_guid AS UNIQUEIDENTIFIER, @device_name AS NVARCHAR(100), @sensor_location AS NVARCHAR(30), @dev_eui AS NVARCHAR(30), @join_eui AS NVARCHAR(30), @dev_addr AS NVARCHAR(15), @device_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	IF EXISTS
	(
		SELECT device_guid
		from dbresprod.dbo.TTN_DEVICES
		WHERE device_name = @device_name
	)
	BEGIN
		SELECT device_guid
		from dbresprod.dbo.TTN_DEVICES
		WHERE device_name = @device_name
	END
	ELSE
		BEGIN
			SET @device_guid = NULL
			SET @device_guid = NEWID()
			INSERT INTO dbresprod.dbo.TTN_DEVICES (device_guid, application_guid, device_name, dev_eui, join_eui, dev_addr) VALUES (@device_guid, @application_guid, @device_name, @dev_eui, @join_eui, @dev_addr)
		END
END
GO


/* Create a new uplink entry, and return GUID. */
CREATE PROCEDURE PROC_CREATE_TTN_UPLINK (@device_guid AS UNIQUEIDENTIFIER, @warning_guid AS UNIQUEIDENTIFIER, @session_key_id AS NVARCHAR(40), @f_port AS INT, @f_cnt AS INT, @frm_payload AS NVARCHAR(50), @raw_bytes AS NVARCHAR(50), @consumed_airtime AS FLOAT, @uplink_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @uplink_guid = NULL
	SET @uplink_guid = NEWID()
	INSERT INTO dbresprod.dbo.TTN_UPLINKS (uplink_guid, device_guid, warning_guid, session_key_id, f_port, f_cnt, frm_payload, raw_bytes, consumed_airtime) VALUES (@uplink_guid, @device_guid, @warning_guid, @session_key_id, @f_port, @f_cnt, @frm_payload, @raw_bytes, @consumed_airtime)
END
GO


/* Create a new datetime entry. */
CREATE PROCEDURE PROC_CREATE_TTN_DATETIME (@rx_guid AS UNIQUEIDENTIFIER, @hop_guid AS UNIQUEIDENTIFIER, @uplink_guid AS UNIQUEIDENTIFIER, @received_at AS DATETIME)
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO dbresprod.dbo.TTN_DATETIMES (rx_guid, hop_guid, uplink_guid, received_at) VALUES (@rx_guid, @hop_guid, @uplink_guid, @received_at)
END
GO


/* Create a new RX entry, and return the GUID. */
CREATE PROCEDURE PROC_CREATE_TTN_RX (@gateway_guid AS UNIQUEIDENTIFIER, @uplink_guid AS UNIQUEIDENTIFIER, @location_guid AS UNIQUEIDENTIFIER, @rx_time AS DATETIME, @rx_timestamp AS INT, @rssi AS INT, @channel_rssi AS INT, @snr AS FLOAT, @message_id AS NVARCHAR(30), @forwarder_net_id AS INT, @forwarder_tenant_id AS NVARCHAR(8), @forwarder_cluster_id AS NVARCHAR(15), @home_network_net_id AS INT, @home_network_tenant_id AS NVARCHAR(8), @home_network_cluster_id AS NVARCHAR(12), @rx_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @rx_guid = NULL
	SET @rx_guid = NEWID()
	INSERT INTO dbresprod.dbo.TTN_RX (rx_guid, gateway_guid, uplink_guid, location_guid, rx_time, rx_timestamp, rssi, channel_rssi, snr, message_id, forwarder_net_id, forwarder_tenant_id, forwarder_cluster_id, home_network_net_id, home_network_tenant_id, home_network_cluster_id) VALUES (@rx_guid, @gateway_guid, @uplink_guid, @location_guid, @rx_time, @rx_timestamp, @rssi, @channel_rssi, @snr, @message_id, @forwarder_net_id, @forwarder_tenant_id, @forwarder_cluster_id, @home_network_net_id, @home_network_tenant_id, @home_network_cluster_id)
END
GO


/* Create a new HOP entry, and return GUID. */
CREATE PROCEDURE PROC_CREATE_TTN_HOP (@gateway_guid AS UNIQUEIDENTIFIER, @rx_guid AS UNIQUEIDENTIFIER, @sender_address AS NVARCHAR(15), @receiver_name AS NVARCHAR(40), @receiver_agent AS NVARCHAR(40), @hop_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @hop_guid = NULL
	SET @hop_guid = NEWID()
	INSERT INTO dbresprod.dbo.TTN_HOPS (hop_guid, gateway_guid, rx_guid, sender_address, receiver_name, receiver_agent) VALUES (@hop_guid, @gateway_guid, @rx_guid, @sender_address, @receiver_name, @receiver_agent)
END
GO


/* Create a new correlation id entry, and return GUID. */
CREATE PROCEDURE PROC_CREATE_TTN_CORRELATION_ID (@rx_guid AS UNIQUEIDENTIFIER, @correlation_id AS NVARCHAR(MAX), @correlation_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @correlation_guid = NULL
	SET @correlation_guid = NEWID()
	INSERT INTO dbresprod.dbo.TTN_CORRELATION_IDS (rx_guid, correlation_id, correlation_guid) VALUES (@rx_guid, @correlation_id, @correlation_guid)
END
GO


/* Create a new uplink token. */
CREATE PROCEDURE PROC_CREATE_TTN_UPLINK_TOKEN (@rx_guid AS UNIQUEIDENTIFIER, @gateway_guid AS UNIQUEIDENTIFIER, @uplink_token AS VARCHAR(MAX))
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO dbresprod.dbo.TTN_UPLINK_TOKENS (rx_guid, gateway_guid, uplink_token) VALUES (@rx_guid, @gateway_guid, @uplink_token)
END
GO


/* Create a new reading entry. */
CREATE PROCEDURE PROC_CREATE_TTN_READING (@uplink_guid AS UNIQUEIDENTIFIER, @sensor_guid AS UNIQUEIDENTIFIER, @sensor_value AS NVARCHAR(MAX))
AS
BEGIN
	SET NOCOUNT ON;
	INSERT INTO dbresprod.dbo.READINGS (uplink_guid, sensor_guid, sensor_value) VALUES (@uplink_guid, @sensor_guid, @sensor_value)
END
GO


/* Create a new uplink setting entry, and return the GUID. */
CREATE PROCEDURE PROC_CREATE_TTN_UPLINK_SETTING (@uplink_guid AS UNIQUEIDENTIFIER, @bandwidth AS INT, @spreading_factor AS INT, @data_rate_index AS INT, @coding_rate AS NVARCHAR(5), @frequency AS INT, @setting_timestamp AS INT, @uplink_setting_guid AS UNIQUEIDENTIFIER OUTPUT)
AS
BEGIN
	SET NOCOUNT ON;
	SET @uplink_setting_guid = NULL
	SET @uplink_setting_guid = NEWID()
	INSERT INTO dbresprod.dbo.TTN_UPLINK_SETTINGS (uplink_setting_guid, uplink_guid, bandwidth, spreading_factor, data_rate_index, coding_rate, frequency, setting_timestamp) VALUES (@uplink_setting_guid, @uplink_guid, @bandwidth, @spreading_factor, @data_rate_index, @coding_rate, @frequency, @setting_timestamp)
END
GO