package za.co.trackmatic.flink.models.volvo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the tachograph driver identification details from Volvo.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VolvoTachoDriverId implements Serializable {
    private String driverIdentification;
    private String cardIssuingMemberState;
    private String driverAuthenticationEquipment;
    private String cardReplacementIndex;
    private String cardRenewalIndex;

    public String getDriverIdentification() {
        return driverIdentification;
    }

    public void setDriverIdentification(String driverIdentification) {
        this.driverIdentification = driverIdentification;
    }

    public String getCardIssuingMemberState() {
        return cardIssuingMemberState;
    }

    public void setCardIssuingMemberState(String cardIssuingMemberState) {
        this.cardIssuingMemberState = cardIssuingMemberState;
    }

    public String getDriverAuthenticationEquipment() {
        return driverAuthenticationEquipment;
    }

    public void setDriverAuthenticationEquipment(String driverAuthenticationEquipment) {
        this.driverAuthenticationEquipment = driverAuthenticationEquipment;
    }

    public String getCardReplacementIndex() {
        return cardReplacementIndex;
    }

    public void setCardReplacementIndex(String cardReplacementIndex) {
        this.cardReplacementIndex = cardReplacementIndex;
    }

    public String getCardRenewalIndex() {
        return cardRenewalIndex;
    }

    public void setCardRenewalIndex(String cardRenewalIndex) {
        this.cardRenewalIndex = cardRenewalIndex;
    }
}
