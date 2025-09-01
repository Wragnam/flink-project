package za.co.trackmatic.flink.models.lynx;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a TRU (Transport Refrigeration Unit) asset with details about
 * its identity, status, control system, tenant ownership, and sensor configuration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Asset implements Serializable {
    private String assetId;
    private String assetName;
    private String licensePlateNumber;
    private String status;
    private String truSerialNumber;
    private String truModelNumber;
    private String truControlSystemType;
    private String truSoftwareVersion;
    private String createdDate;
    private String tenantId;
    private String tenantName;
    private String deviceId;
    private List<SensorConfiguration> sensorConfiguration;

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getAssetName() {
        return assetName;
    }

    public void setAssetName(String assetName) {
        this.assetName = assetName;
    }

    public String getLicensePlateNumber() {
        return licensePlateNumber;
    }

    public void setLicensePlateNumber(String licensePlateNumber) {
        this.licensePlateNumber = licensePlateNumber;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTruSerialNumber() {
        return truSerialNumber;
    }

    public void setTruSerialNumber(String truSerialNumber) {
        this.truSerialNumber = truSerialNumber;
    }

    public String getTruModelNumber() {
        return truModelNumber;
    }

    public void setTruModelNumber(String truModelNumber) {
        this.truModelNumber = truModelNumber;
    }

    public String getTruControlSystemType() {
        return truControlSystemType;
    }

    public void setTruControlSystemType(String truControlSystemType) {
        this.truControlSystemType = truControlSystemType;
    }

    public String getTruSoftwareVersion() {
        return truSoftwareVersion;
    }

    public void setTruSoftwareVersion(String truSoftwareVersion) {
        this.truSoftwareVersion = truSoftwareVersion;
    }

    public String getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(String createdDate) {
        this.createdDate = createdDate;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTenantName() {
        return tenantName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public List<SensorConfiguration> getSensorConfiguration() {
        return sensorConfiguration;
    }

    public void setSensorConfiguration(List<SensorConfiguration> sensorConfiguration) {
        this.sensorConfiguration = sensorConfiguration;
    }
}
