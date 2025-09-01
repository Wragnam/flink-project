package za.co.trackmatic.flink.Utils;

import java.io.Serializable;

/**
 * Defines constant string identifiers representing various data sources.
 * <p>
 * These constants are used throughout the system to identify and differentiate
 * between different telemetry or event data providers.
 */
public class SourceMapping implements Serializable {
    public static final String BLACKBERRYRADAR = "BLACKBERRYRADAR";
    public static final String CARTRACK = "CARTRACK";
    public static final String FLEETBOARD = "FLEETBOARD";
    public static final String ICAM = "ICAM";
    public static final String LYNX = "LYNX";
    public static final String LYTX = "LYTX";
    public static final String MIX = "MIX_TELEMATICS";
    public static final String PARAGON = "TRACKMATIC";
    public static final String SURFSIGHT = "SURFSIGHT";
    public static final String THERMOKING = "THERMOKING";
    public static final String UD_TRUCKS = "UD_TRUCKS";
    public static final String VOLVO = "VOLVO";
    public static final String SCANIA = "SCANIA";
    public static final String RMS = "RMS";
}
