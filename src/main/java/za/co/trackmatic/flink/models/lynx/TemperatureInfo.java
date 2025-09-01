package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents temperature-related information collected from various sensors,
 * including setpoint temperatures, return/supply air temperatures, datacold,
 * Bluetooth, and on-wire temperature readings.
 * <p>
 * This class is used primarily for deserializing JSON payloads in Flink jobs.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TemperatureInfo implements Serializable {
    private Double setpointTemp1;
    private Double setpointTemp2;
    private Double setpointTemp3;
    private Double returnAirTemp1;
    private Double returnAirTemp2;
    private Double returnAirTemp3;
    private Double supplyAirTemp1;
    private Double supplyAirTemp2;
    private Double supplyAirTemp3;
    private Double datacoldTemp1;
    private Double datacoldTemp2;
    private Double datacoldTemp3;
    private Double datacoldTemp4;
    private Double datacoldTemp5;
    private Double datacoldTemp6;
    private Double bluetoothTemp1;
    private Double bluetoothTemp2;
    private Double bluetoothTemp3;
    private Double bluetoothTemp4;
    private Double bluetoothTemp5;
    private Double bluetoothTemp6;
    private Double onWireTemp1;
    private Double onWireTemp2;
    private Double onWireTemp3;
    private Double onWireTemp4;
    private Double onWireTemp5;
    private Double onWireTemp6;

    public Double getSetpointTemp1() {
        return setpointTemp1;
    }

    public void setSetpointTemp1(Double setpointTemp1) {
        this.setpointTemp1 = setpointTemp1;
    }

    public Double getSetpointTemp2() {
        return setpointTemp2;
    }

    public void setSetpointTemp2(Double setpointTemp2) {
        this.setpointTemp2 = setpointTemp2;
    }

    public Double getSetpointTemp3() {
        return setpointTemp3;
    }

    public void setSetpointTemp3(Double setpointTemp3) {
        this.setpointTemp3 = setpointTemp3;
    }

    public Double getReturnAirTemp1() {
        return returnAirTemp1;
    }

    public void setReturnAirTemp1(Double returnAirTemp1) {
        this.returnAirTemp1 = returnAirTemp1;
    }

    public Double getReturnAirTemp2() {
        return returnAirTemp2;
    }

    public void setReturnAirTemp2(Double returnAirTemp2) {
        this.returnAirTemp2 = returnAirTemp2;
    }

    public Double getReturnAirTemp3() {
        return returnAirTemp3;
    }

    public void setReturnAirTemp3(Double returnAirTemp3) {
        this.returnAirTemp3 = returnAirTemp3;
    }

    public Double getSupplyAirTemp1() {
        return supplyAirTemp1;
    }

    public void setSupplyAirTemp1(Double supplyAirTemp1) {
        this.supplyAirTemp1 = supplyAirTemp1;
    }

    public Double getSupplyAirTemp2() {
        return supplyAirTemp2;
    }

    public void setSupplyAirTemp2(Double supplyAirTemp2) {
        this.supplyAirTemp2 = supplyAirTemp2;
    }

    public Double getSupplyAirTemp3() {
        return supplyAirTemp3;
    }

    public void setSupplyAirTemp3(Double supplyAirTemp3) {
        this.supplyAirTemp3 = supplyAirTemp3;
    }

    public Double getDatacoldTemp1() {
        return datacoldTemp1;
    }

    public void setDatacoldTemp1(Double datacoldTemp1) {
        this.datacoldTemp1 = datacoldTemp1;
    }

    public Double getDatacoldTemp2() {
        return datacoldTemp2;
    }

    public void setDatacoldTemp2(Double datacoldTemp2) {
        this.datacoldTemp2 = datacoldTemp2;
    }

    public Double getDatacoldTemp3() {
        return datacoldTemp3;
    }

    public void setDatacoldTemp3(Double datacoldTemp3) {
        this.datacoldTemp3 = datacoldTemp3;
    }

    public Double getDatacoldTemp4() {
        return datacoldTemp4;
    }

    public void setDatacoldTemp4(Double datacoldTemp4) {
        this.datacoldTemp4 = datacoldTemp4;
    }

    public Double getDatacoldTemp5() {
        return datacoldTemp5;
    }

    public void setDatacoldTemp5(Double datacoldTemp5) {
        this.datacoldTemp5 = datacoldTemp5;
    }

    public Double getDatacoldTemp6() {
        return datacoldTemp6;
    }

    public void setDatacoldTemp6(Double datacoldTemp6) {
        this.datacoldTemp6 = datacoldTemp6;
    }

    public Double getBluetoothTemp1() {
        return bluetoothTemp1;
    }

    public void setBluetoothTemp1(Double bluetoothTemp1) {
        this.bluetoothTemp1 = bluetoothTemp1;
    }

    public Double getBluetoothTemp2() {
        return bluetoothTemp2;
    }

    public void setBluetoothTemp2(Double bluetoothTemp2) {
        this.bluetoothTemp2 = bluetoothTemp2;
    }

    public Double getBluetoothTemp3() {
        return bluetoothTemp3;
    }

    public void setBluetoothTemp3(Double bluetoothTemp3) {
        this.bluetoothTemp3 = bluetoothTemp3;
    }

    public Double getBluetoothTemp4() {
        return bluetoothTemp4;
    }

    public void setBluetoothTemp4(Double bluetoothTemp4) {
        this.bluetoothTemp4 = bluetoothTemp4;
    }

    public Double getBluetoothTemp5() {
        return bluetoothTemp5;
    }

    public void setBluetoothTemp5(Double bluetoothTemp5) {
        this.bluetoothTemp5 = bluetoothTemp5;
    }

    public Double getBluetoothTemp6() {
        return bluetoothTemp6;
    }

    public void setBluetoothTemp6(Double bluetoothTemp6) {
        this.bluetoothTemp6 = bluetoothTemp6;
    }

    public Double getOnWireTemp1() {
        return onWireTemp1;
    }

    public void setOnWireTemp1(Double onWireTemp1) {
        this.onWireTemp1 = onWireTemp1;
    }

    public Double getOnWireTemp2() {
        return onWireTemp2;
    }

    public void setOnWireTemp2(Double onWireTemp2) {
        this.onWireTemp2 = onWireTemp2;
    }

    public Double getOnWireTemp3() {
        return onWireTemp3;
    }

    public void setOnWireTemp3(Double onWireTemp3) {
        this.onWireTemp3 = onWireTemp3;
    }

    public Double getOnWireTemp4() {
        return onWireTemp4;
    }

    public void setOnWireTemp4(Double onWireTemp4) {
        this.onWireTemp4 = onWireTemp4;
    }

    public Double getOnWireTemp5() {
        return onWireTemp5;
    }

    public void setOnWireTemp5(Double onWireTemp5) {
        this.onWireTemp5 = onWireTemp5;
    }

    public Double getOnWireTemp6() {
        return onWireTemp6;
    }

    public void setOnWireTemp6(Double onWireTemp6) {
        this.onWireTemp6 = onWireTemp6;
    }
}
