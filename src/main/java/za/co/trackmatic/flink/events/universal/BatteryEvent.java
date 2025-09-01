package za.co.trackmatic.flink.events.universal;

import java.io.Serializable;

/**
 * Represents a universal battery event.
 */
public class BatteryEvent implements Serializable {

    public BatteryEvent() {
    }

    /**
     * The type of battery. Usually MAIN_BATTERY or BACKUP_BATTERY.
     */
    private String batteryType;

    /**
     * The voltage reading of the battery.
     */
    private double voltage;

    public String getBatteryType() {
        return batteryType;
    }

    public void setBatteryType(String batteryType) {
        this.batteryType = batteryType;
    }

    public double getVoltage() {
        return voltage;
    }

    public void setVoltage(double voltage) {
        this.voltage = voltage;
    }
}
