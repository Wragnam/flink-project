package za.co.trackmatic.flink.models.cartrack;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents a driver entity from Cartrack system.
 * <p>
 * Contains driver identification and contact details.
 * JSON properties are mapped to Java fields for serialization/deserialization.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CartrackDriver implements Serializable {

    @JsonProperty("driver_id")
    private String driverId;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("last_name")
    private String lastName;

    @JsonProperty("id_number")
    private String idNumber;

    @JsonProperty("license_number")
    private String licenseNumber;

    @JsonProperty("tag_id")
    private String tagId;

    @JsonProperty("phone_number")
    private String phoneNumber;

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getIdNumber() {
        return idNumber;
    }

    public void setIdNumber(String idNumber) {
        this.idNumber = idNumber;
    }

    public String getLicenseNumber() {
        return licenseNumber;
    }

    public void setLicenseNumber(String licenseNumber) {
        this.licenseNumber = licenseNumber;
    }

    public String getTagId() {
        return tagId;
    }

    public void setTagId(String tagId) {
        this.tagId = tagId;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
}
