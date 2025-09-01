package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a collection of Trackmatic devices and related data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TmDevices implements Serializable {

    /**
     * Represents a single Trackmatic device with its associated metadata,
     * including organization, asset, active load, and configuration details.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TmDevice implements Serializable {

        /**
         * Unique identifier of the device.
         */
        private String id;

        /**
         * Organization identifier to which the device belongs.
         */
        private String orgId;

        /**
         * Device identifier.
         */
        private String deviceId;

        /**
         * Asset identifier associated with this device.
         */
        private String assetId;

        /**
         * List of site IDs associated with this device.
         */
        private List<String> siteIds;

        /**
         * List of control rooms linked to this device.
         */
        private List<ControlRoom> controlRooms;

        /**
         * Active load associated with this device.
         */
        private ActiveLoad tpsActiveLoad;

        /**
         * Organization configuration related to this device.
         */
        private OrgConfiguration tpsOrgConfig;

        public TmDevice() {

        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getOrgId() {
            return orgId;
        }

        public void setOrgId(String orgId) {
            this.orgId = orgId;
        }

        public String getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(String deviceId) {
            this.deviceId = deviceId;
        }

        public String getAssetId() {
            return assetId;
        }

        public void setAssetId(String assetId) {
            this.assetId = assetId;
        }

        public List<String> getSiteIds() {
            return siteIds;
        }

        public void setSiteIds(List<String> siteIds) {
            this.siteIds = siteIds;
        }

        public List<ControlRoom> getControlRooms() {
            return controlRooms;
        }

        public void setControlRooms(List<ControlRoom> controlRooms) {
            this.controlRooms = controlRooms;
        }

        public ActiveLoad getTpsActiveLoad() {
            return tpsActiveLoad;
        }

        public void setTpsActiveLoad(ActiveLoad tpsActiveLoad) {
            this.tpsActiveLoad = tpsActiveLoad;
        }

        public OrgConfiguration getTpsOrgConfig() {
            return tpsOrgConfig;
        }

        public void setTpsOrgConfig(OrgConfiguration tpsOrgConfig) {
            this.tpsOrgConfig = tpsOrgConfig;
        }
    }

    /**
     * Wrapper class for the list of TmDevices, usually used for JSON deserialization.
     */
    public static class GetTmDeviceWrapper {

        /**
         * List of TmDevice objects returned by the API.
         */
        private List<TmDevice> getTpsDevices;

        public GetTmDeviceWrapper() {

        }

        public List<TmDevice> getGetTpsDevices() {
            return getTpsDevices;
        }

        public void setGetTpsDevices(List<TmDevice> getTpsDevices) {
            this.getTpsDevices = getTpsDevices;
        }
    }

    /**
     * The actual data containing the wrapper object.
     */
    private GetTmDeviceWrapper data;

    public TmDevices() {
    }

    public GetTmDeviceWrapper getData() {
        return data;
    }

    public void setData(GetTmDeviceWrapper data) {
        this.data = data;
    }
}
