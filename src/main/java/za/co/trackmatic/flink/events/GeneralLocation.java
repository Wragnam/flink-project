package za.co.trackmatic.flink.events;

import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;

/**
 * Represents a general location event that includes basic metadata and timestamp.
 */
public class GeneralLocation extends EventBase implements Serializable {

    /**
     * Constructs a {@code GeneralLocation} event with the specified parameters.
     *
     * @param lat          the latitude
     * @param lon          the longitude
     * @param timestamp    the timestamp of the event
     * @param source       the data source
     * @param metadata     the metadata associated with the device
     * @param deviceId     the device ID
     * @param deviceSerial the device serial number
     */
    public GeneralLocation(Double lat, Double lon, long timestamp,
                           String source, TmMetadata metadata, String deviceId, String deviceSerial) {
        super(lat, lon, source);

        populateBasicData(deviceId, metadata, deviceSerial);

        this.setTimestamp(timestamp);
    }
}
