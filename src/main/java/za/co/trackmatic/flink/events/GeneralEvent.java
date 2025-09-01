package za.co.trackmatic.flink.events;

import za.co.trackmatic.flink.Utils.EventUtils;
import za.co.trackmatic.flink.events.lytx.LytxSpecificEventData;
import za.co.trackmatic.flink.events.mix.MixSpecificData;
import za.co.trackmatic.flink.events.paragon.AccidentEvent;
import za.co.trackmatic.flink.events.universal.BatteryEvent;
import za.co.trackmatic.flink.events.surfsight.SurfsightSpecificData;
import za.co.trackmatic.flink.models.MultipleProviders.DoorStateEvent;
import za.co.trackmatic.flink.models.MultipleProviders.LowFuelEvent;
import za.co.trackmatic.flink.models.MultipleProviders.SetPointEvent;
import za.co.trackmatic.flink.models.trackmatic.GeofenceEvent;
import za.co.trackmatic.flink.models.trackmatic.Load;
import za.co.trackmatic.flink.models.trackmatic.StopEvent;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

/**
 * Represents a general event that may include multiple types of provider-specific data.
 */
public class GeneralEvent extends EventBase implements Serializable {

    /**
     * Unique identifier for the event.
     */
    private String id = UUID.randomUUID().toString();

    /**
     * The source type of the event.
     * Before mapped to Trackmatic side events.
     */
    private String sourceType;

    /**
     * The Trackmatic-standardized event type.
     */
    private String type;

    /**
     * Set point event data.
     * Created when the setpoint between points differ.
     */
    private SetPointEvent setPointEvent;

    /**
     * Door state event data.
     * Created when doors open and close.
     */
    private DoorStateEvent doorStateEvent;

    /**
     * Low fuel event data.
     */
    private LowFuelEvent lowFuelEvent;

    /**
     * Geofence event data.
     */
    private GeofenceEvent geofenceEvent;

    /**
     * Battery event data.
     */
    private BatteryEvent batteryEvent;

    /**
     * Unknown stop event data.
     */
    private StopEvent unknownStopEvent;

    /**
     * Accident event data.
     */
    private AccidentEvent accidentEvent;

    /**
     * Associated site IDs.
     */
    private List<String> siteIds;

    /**
     * Associated control room IDs.
     */
    private List<String> controlRoomIds;

    /**
     * Load information associated with the device.
     */
    private Load load;

    /**
     * Timestamp of when the event was created in seconds.
     */
    private long created;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public SetPointEvent getSetPointEvent() {
        return setPointEvent;
    }

    public void setSetPointEvent(SetPointEvent setPointEvent) {
        this.setPointEvent = setPointEvent;
    }

    public DoorStateEvent getDoorStateEvent() {
        return doorStateEvent;
    }

    public void setDoorStateEvent(DoorStateEvent doorStateEvent) {
        this.doorStateEvent = doorStateEvent;
    }

    public LowFuelEvent getLowFuelEvent() {
        return lowFuelEvent;
    }

    public void setLowFuelEvent(LowFuelEvent lowFuelEvent) {
        this.lowFuelEvent = lowFuelEvent;
    }

    public GeofenceEvent getGeofenceEvent() {
        return geofenceEvent;
    }

    public void setGeofenceEvent(GeofenceEvent geofenceEvent) {
        this.geofenceEvent = geofenceEvent;
    }

    public BatteryEvent getBatteryEvent() {
        return batteryEvent;
    }

    public void setBatteryEvent(BatteryEvent batteryEvent) {
        this.batteryEvent = batteryEvent;
    }

    public StopEvent getUnknownStopEvent() {
        return unknownStopEvent;
    }

    public void setUnknownStopEvent(StopEvent unknownStopEvent) {
        this.unknownStopEvent = unknownStopEvent;
    }

    public AccidentEvent getAccidentEvent() {
        return accidentEvent;
    }

    public void setAccidentEvent(AccidentEvent accidentEvent) {
        this.accidentEvent = accidentEvent;
    }

    public List<String> getSiteIds() {
        return siteIds;
    }

    public void setSiteIds(List<String> siteIds) {
        this.siteIds = siteIds;
    }

    public List<String> getControlRoomIds() {
        return controlRoomIds;
    }

    public void setControlRoomIds(List<String> controlRoomIds) {
        this.controlRoomIds = controlRoomIds;
    }

    public Load getLoad() {
        return load;
    }

    public void setLoad(Load load) {
        this.load = load;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    /**
     * Constructs a {@code GeneralEvent} with the specified parameters.
     *
     * @param lat          latitude
     * @param lon          longitude
     * @param created      creation timestamp
     * @param source       source string
     * @param metadata     metadata object
     * @param deviceId     device ID
     * @param deviceSerial device serial number
     */
    public GeneralEvent(Double lat, Double lon, long created, String source, TmMetadata metadata, String deviceId,
                        String deviceSerial) {
        super(lat, lon, source);

        this.setCreated(created);

        this.setSiteIds(metadata.getSiteIds());

        this.setLoad(EventUtils.createLoad(metadata));

        populateBasicData(deviceId, metadata, deviceSerial);
    }

    /**
     * Lytx-specific event data.
     */
    private LytxSpecificEventData lytx;

    public LytxSpecificEventData getLytx() {
        return lytx;
    }

    public void setLytx(LytxSpecificEventData lytx) {
        this.lytx = lytx;
    }

    /**
     * Mix-specific event data.
     */
    private MixSpecificData mix;

    public MixSpecificData getMix() {
        return mix;
    }

    public void setMix(MixSpecificData mix) {
        this.mix = mix;
    }

    /**
     * Surfsight-specific event data.
     */
    private SurfsightSpecificData surfsight;

    public SurfsightSpecificData getSurfsight() {
        return surfsight;
    }

    public void setSurfsight(SurfsightSpecificData surfsight) {
        this.surfsight = surfsight;
    }
}
