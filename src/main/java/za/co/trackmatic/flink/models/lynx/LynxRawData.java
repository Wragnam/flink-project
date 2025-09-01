package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the raw telemetry data received from a Lynx device. This class encapsulates
 * information about the event timestamp, asset details, status, temperature, position,
 * alarms, truck connection, and metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LynxRawData implements Serializable {
    private String eventDateTimeUTC;
    private Asset assetInfo;
    private StatusInfo statusInfo;
    private TemperatureInfo temperatureInfo;
    private PositionInfo positionInfo;
    private List<AlarmInfo> alarmInfo;
    private ConnectedTruckInfo connectedTruckInfo;
    private TmMetadata metadata;

    public String getEventDateTimeUTC() {
        return eventDateTimeUTC;
    }

    public void setEventDateTimeUTC(String eventDateTimeUTC) {
        this.eventDateTimeUTC = eventDateTimeUTC;
    }

    public Asset getAssetInfo() {
        return assetInfo;
    }

    public void setAssetInfo(Asset assetInfo) {
        this.assetInfo = assetInfo;
    }

    public StatusInfo getStatusInfo() {
        return statusInfo;
    }

    public void setStatusInfo(StatusInfo statusInfo) {
        this.statusInfo = statusInfo;
    }

    public TemperatureInfo getTemperatureInfo() {
        return temperatureInfo;
    }

    public void setTemperatureInfo(TemperatureInfo temperatureInfo) {
        this.temperatureInfo = temperatureInfo;
    }

    public PositionInfo getPositionInfo() {
        return positionInfo;
    }

    public void setPositionInfo(PositionInfo positionInfo) {
        this.positionInfo = positionInfo;
    }

    public List<AlarmInfo> getAlarmInfo() {
        return alarmInfo;
    }

    public void setAlarmInfo(List<AlarmInfo> alarmInfo) {
        this.alarmInfo = alarmInfo;
    }

    public ConnectedTruckInfo getConnectedTruckInfo() {
        return connectedTruckInfo;
    }

    public void setConnectedTruckInfo(ConnectedTruckInfo connectedTruckInfo) {
        this.connectedTruckInfo = connectedTruckInfo;
    }

    public TmMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(TmMetadata metadata) {
        this.metadata = metadata;
    }
}
