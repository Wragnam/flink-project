package za.co.trackmatic.flink.models.mappingAPI;

import java.io.Serializable;

/**
 * Represents the physical constraints of a truck used for routing and mapping calculations.
 * <p>
 * Includes measurements such as height, width, total weight, and axle weight,
 * which can affect route optimization and road eligibility.
 * </p>
 */
public class TruckConstraints implements Serializable {

    public TruckConstraints() {
        this.axleWeightInTones = 0;
        this.heightInMetres = 0;
        this.weightInTones = 0;
        this.widthInMetres = 0;
    }

    private double heightInMetres;

    private double widthInMetres;

    private double weightInTones;

    private double axleWeightInTones;

    public double getHeightInMetres() {
        return heightInMetres;
    }

    public void setHeightInMetres(double heightInMetres) {
        this.heightInMetres = heightInMetres;
    }

    public double getWidthInMetres() {
        return widthInMetres;
    }

    public void setWidthInMetres(double widthInMetres) {
        this.widthInMetres = widthInMetres;
    }

    public double getWeightInTones() {
        return weightInTones;
    }

    public void setWeightInTones(double weightInTones) {
        this.weightInTones = weightInTones;
    }

    public double getAxleWeightInTones() {
        return axleWeightInTones;
    }

    public void setAxleWeightInTones(double axleWeightInTones) {
        this.axleWeightInTones = axleWeightInTones;
    }
}
