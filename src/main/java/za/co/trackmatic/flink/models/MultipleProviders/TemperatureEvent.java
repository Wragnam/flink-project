package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a temperature-related event with potentially multiple temperature readings.
 * <p>
 * Extends {@link GeneralEvent} and adds specific data related to temperature sensors
 * such as set points, discharge air, and return air temperatures.
 * </p>
 */
public class TemperatureEvent extends GeneralEvent implements Serializable {

    /** The general event type string for temperature events. */
    public static final String TEMPERATURE_TYPE = "temperature";

    /** Subtype identifier for temperature sensor 1. */
    public static final String TEMPERATURE1_SUBTYPE = "temp1";

    /** Subtype identifier for temperature sensor 2. */
    public static final String TEMPERATURE2_SUBTYPE = "temp2";

    /** Subtype identifier for temperature sensor 3. */
    public static final String TEMPERATURE3_SUBTYPE = "temp3";

    /** Source identifier for the temperature event. */
    private String source;

    /** Location ID where the temperature event was recorded. */
    private String locationId;

    /**
     * Holds temperature sensor data including set point, discharge air, and return air readings.
     */
    public static class TemperatureEventData implements Serializable {

        /** The subtype of the temperature sensor (e.g., temp1, temp2, temp3). */
        private String subtype;

        /** The set point temperature value. */
        private Double setPoint;

        /** The return air temperature value. */
        private Double returnAir;

        /** The discharge air temperature value. */
        private Double dischargeAir;

        /**
         * Constructs a TemperatureEventData object.
         *
         * @param subtype the subtype identifier of the temperature sensor
         * @param setPoint the set point temperature value
         * @param dischargeAir the discharge air temperature value
         * @param returnAir the return air temperature value
         */
        public TemperatureEventData(String subtype, Double setPoint, Double dischargeAir, Double returnAir) {
            this.subtype = subtype;
            this.setPoint = setPoint;
            this.dischargeAir = dischargeAir;
            this.returnAir = returnAir;
        }

        public String getSubtype() {
            return subtype;
        }

        public void setSubtype(String subtype) {
            this.subtype = subtype;
        }

        public Double getSetPoint() {
            return setPoint;
        }

        public void setSetPoint(Double setPoint) {
            this.setPoint = setPoint;
        }

        public Double getReturnAir() {
            return returnAir;
        }

        public void setReturnAir(Double returnAir) {
            this.returnAir = returnAir;
        }

        public Double getDischargeAir() {
            return dischargeAir;
        }

        public void setDischargeAir(Double dischargeAir) {
            this.dischargeAir = dischargeAir;
        }
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getLocationId() {
        return locationId;
    }

    public void setLocationId(String locationId) {
        this.locationId = locationId;
    }

    private List<TemperatureEventData> temperatureData = new ArrayList<>();

    /**
     * Constructs a TemperatureEvent.
     *
     * @param type the event type, expected to be {@link #TEMPERATURE_TYPE}
     * @param latitude latitude where event was recorded
     * @param longitude longitude where event was recorded
     * @param created timestamp of event creation in milliseconds
     * @param serial serial number or identifier associated with the event source
     * @param metadata metadata associated with the event
     * @param source source identifier for the event
     */
    public TemperatureEvent(String type, double latitude, double longitude, long created, String serial, TmMetadata metadata, String source) {
        super(latitude, longitude, created, source, metadata, serial, serial);

        setType(type);
    }

    public List<TemperatureEventData> getTemperatureData() {
        return temperatureData;
    }

    public void setTemperatureData(List<TemperatureEventData> temperatureData) {
        this.temperatureData = temperatureData;
    }
}
