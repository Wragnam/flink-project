package za.co.trackmatic.flink.models.trips;

import java.io.Serializable;

/**
 * Represents the details of a driver involved in a trip.
 * <p>
 * This includes identification numbers, initials, and surname,
 * typically used for associating trip events with a specific individual.
 * </p>
 */
public class DriverDetails implements Serializable {
    public DriverDetails(){}

    /**
     * The driver's license or permit document number.
     */
    private String documentNumber;

    /**
     * The unique identifier of the driver.
     */
    private String id;

    /**
     * The national identity number of the driver.
     */
    private String identityNumber;

    /**
     * The driver's initials (e.g., J.P.).
     */
    private String initials;

    /**
     * The driver's last name or surname.
     */
    private String lastName;

    public String getDocumentNumber() {
        return documentNumber;
    }

    public void setDocumentNumber(String documentNumber) {
        this.documentNumber = documentNumber;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdentityNumber() {
        return identityNumber;
    }

    public void setIdentityNumber(String identityNumber) {
        this.identityNumber = identityNumber;
    }

    public String getInitials() {
        return initials;
    }

    public void setInitials(String initials) {
        this.initials = initials;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
}
