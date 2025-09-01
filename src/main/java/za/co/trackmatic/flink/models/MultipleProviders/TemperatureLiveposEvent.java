package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.events.GeofenceData;
import za.co.trackmatic.flink.models.lynx.LynxRawData;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;

import java.io.Serializable;
import java.text.ParseException;
import java.util.List;

/**
 * Represents a real-time temperature event generated from various telematics data providers.
 * <p>
 * This event contains temperature readings, location data, geofence context, and motion status.
 * </p>
 */
public class TemperatureLiveposEvent extends GeneralEvent implements Serializable {

    /** Identifier of the data source (e.g., THERMOKING, LYNX, TRACKMATIC). */
    private String source;

    /** List of temperature-related data points such as setpoint, return air, and discharge air. */
    private List<TemperatureEvent.TemperatureEventData> temperatureData;

    /** Geofence information associated with the event. */
    private GeofenceData geofenceData;

    /** Indicates whether the asset is moving, stationary or speeding. */
    private String movingStatus;

    /** Timestamp in formatted string form. */
    private String dateTime;

    /**
     * Constructs a TemperatureLiveposEvent using Thermo King data.
     *
     * @param data ThermokingData object containing GPS and temperature data
     */
    public TemperatureLiveposEvent(ThermokingData data) {
        super(data.getLatitude(), data.getLongitude(), Utils.getCreatedTimeFromThermokingData(data), "THERMOKING", data.getTmMetadata(),
                Utils.cleanSerial(data.getReeferSerialNumber()), Utils.cleanSerial(data.getReeferSerialNumber()));
    }

    /**
     * Constructs a TemperatureLiveposEvent using LynxRawData.
     *
     * @param rawData the raw Lynx data
     * @throws ParseException if the datetime format is invalid
     */
    public TemperatureLiveposEvent(LynxRawData rawData) throws ParseException {
        super(Double.parseDouble(rawData.getPositionInfo().getLat()), Double.parseDouble(rawData.getPositionInfo().getLon()),
                Utils.getTimeFromString(rawData.getEventDateTimeUTC(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), "LYNX", rawData.getMetadata(),
                Utils.cleanSerial(rawData.getAssetInfo().getTruSerialNumber()), Utils.cleanSerial(rawData.getAssetInfo().getTruSerialNumber()));
    }

    /**
     * Constructs a TemperatureLiveposEvent using ParagonRawData.
     *
     * @param rawData the raw Paragon data
     * @param lat latitude coordinate
     * @param lon longitude coordinate
     * @param dateTime timestamp in milliseconds
     * @throws ParseException if the datetime format is invalid
     */
    public TemperatureLiveposEvent(ParagonRawData rawData,double lat, double lon, long dateTime) throws ParseException{
        super(lat,lon, dateTime, "TRACKMATIC", rawData.getMetadata(), rawData.getSerial(), rawData.getSerial());
    }

    public List<TemperatureEvent.TemperatureEventData> getTemperatureData() {
        return temperatureData;
    }

    public void setTemperatureData(List<TemperatureEvent.TemperatureEventData> temperatureData) {
        this.temperatureData = temperatureData;
    }

    public GeofenceData getGeofenceData() {
        return geofenceData;
    }

    public void setGeofenceData(GeofenceData geofenceData) {
        this.geofenceData = geofenceData;
    }

    public String getMovingStatus() {
        return movingStatus;
    }

    public void setMovingStatus(String movingStatus) {
        this.movingStatus = movingStatus;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }
}
