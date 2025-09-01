package za.co.trackmatic.flink.models.MultipleProviders;

import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.TripProcessingUtils;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.cartrack.CartrackPositionRaw;
import za.co.trackmatic.flink.models.icam.IcamPositionRaw;
import za.co.trackmatic.flink.models.lynx.LynxRawData;
import za.co.trackmatic.flink.models.rms.RmsLiveposRaw;
import za.co.trackmatic.flink.models.scania.ScaniaPositionRaw;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.models.volvo.VolvoLiveposRaw;

import java.io.Serializable;
import java.text.ParseException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * Utility class providing mapping functions to convert raw vehicle tracking data from various providers
 * into unified internal representations (`RawTripData` and `RawLiveposData`).
 * <p>
 * This includes data from:
 * <ul>
 *     <li>Scania</li>
 *     <li>Icam</li>
 *     <li>Cartrack</li>
 *     <li>Volvo</li>
 *     <li>Thermoking</li>
 *     <li>Lynx</li>
 * </ul>
 * The class is stateless and thread-safe.
 */
public final class TripAndEventMappers implements Serializable {

    // --- Time Zone and Date Formatting Constants ---

    /** Zone offset +2. */
    private static final ZoneOffset plusTwo = ZoneOffset.ofHours(2);

    /** Format: 2025-07-14 14:46:07+02 */
    private static final String dF1 = "yyyy-MM-dd HH:mm:ssXXX";

    /** Format: 2025-07-14T07:01:36.000Z */
    private static final String dF2 = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

    /** Format: 2025-06-30T00:42:06 */
    private static final String dF3 = "yyyy-MM-dd'T'HH:mm:ss";

    /** Format: 2025-07-16T11:03:20.000+00:00 */
    private static final String dF4 = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private static final DateTimeFormatter dtF1 =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern(dF1)
                    .toFormatter()
                    .withZone(ZoneOffset.UTC);

    //Set to use zone offset of +2
    private static final DateTimeFormatter dtF3_original =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern(dF3)
                    .toFormatter()
                    .withZone(plusTwo);
    private static final DateTimeFormatter dtF3_utc =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern(dF3)
                    .toFormatter()
                    .withZone(ZoneOffset.UTC);

    // --- SCANIA MAPPERS ---

    /**
     * Converts a {@link ScaniaPositionRaw} into a {@link RawTripData.Builder}.
     *
     * @param s the ScaniaPositionRaw instance to convert
     * @return a {@link RawTripData.Builder} initialized with data from the ScaniaPositionRaw
     * @throws ParseException if the date parsing fails
     */
    public static RawTripData.Builder fromScania(ScaniaPositionRaw s) throws ParseException {
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(s.getVin()))
                .latitude(s.getGnssPosition().getLatitude())
                .longitude(s.getGnssPosition().getLongitude())
                .speed(s.getWheelBasedSpeed())
                .direction(s.getGnssPosition().getHeading() != null ?
                        Double.valueOf(s.getGnssPosition().getHeading()) : null)
                .dateString(s.getCreatedDateTime())
                .dateFormat(dF3)
                .metadata(s.getMetadata())
                .utcTimeValue(TripProcessingUtils.returnTimeInSeconds(s.getCreatedDateTime(), 1, dF3))
                .ignitionString(s.getTriggerType().getTriggerType())
                .source(SourceMapping.SCANIA);
    }
    /**
     * Converts a {@link ScaniaPositionRaw} into a finalized {@link RawLiveposData}
     * for live position event processing.
     *
     * @param s the ScaniaPositionRaw instance to convert
     * @return a {@link RawLiveposData} object built from the given ScaniaPositionRaw
     * @throws ParseException if the date parsing fails
     */
    public static RawLiveposData fromScaniaForEvent(ScaniaPositionRaw s) throws ParseException {
        return new RawLiveposData.Builder(fromScania(s))
                .deviceId(Utils.cleanSerial(s.getVin()))
                .created(TripProcessingUtils.returnTimeInSeconds(s.getCreatedDateTime(), 1000,dF3))
                .build();
    }

    // --- ICAM MAPPERS ---

    /**
     * Converts an {@link IcamPositionRaw} into a {@link RawTripData.Builder}.
     *
     * @param i the IcamPositionRaw instance to convert
     * @return a {@link RawTripData.Builder} initialized with data from the IcamPositionRaw
     */
    public static RawTripData.Builder fromIcam(IcamPositionRaw i) {
        String date = i.getTimestamp();
        long utcTime = OffsetDateTime.parse(date, dtF3_original).toInstant().toEpochMilli();
        String dateString = dtF3_utc.format(Instant.ofEpochMilli(utcTime));
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(String.valueOf(i.getId())))
                .latitude(i.getLatitude())
                .longitude(i.getLongitude())
                .speed(i.getSpeed())
                .direction(i.getCourse())
                .dateString(dateString)
                .dateFormat(dF3)
                .metadata(i.getMetadata())
                .utcTimeValue(utcTime)
                .source(SourceMapping.ICAM);
    }

    /**
     * Converts an {@link IcamPositionRaw} into a finalized {@link RawLiveposData} for event processing.
     *
     * @param i the IcamPositionRaw instance to convert
     * @return a {@link RawLiveposData} object built from the given IcamPositionRaw
     */
    public static RawLiveposData fromIcamForEvent(IcamPositionRaw i) {
        return new RawLiveposData.Builder(fromIcam(i))
                .deviceId(Utils.cleanSerial(String.valueOf(i.getId())))
                .created(OffsetDateTime.parse(i.getTimestamp(), dtF3_original)
                        .toInstant().getEpochSecond())
                .build();
    }

    // --- CARTRACK MAPPERS ---

    /**
     * Converts a {@link CartrackPositionRaw} into a {@link RawTripData.Builder}.
     *
     * @param c the CartrackPositionRaw instance to convert
     * @return a {@link RawTripData.Builder} initialized with data from the CartrackPositionRaw
     */
    public static RawTripData.Builder fromCartrack(CartrackPositionRaw c) {
        String date = Utils.normalizeDateOffset(c.getEventTs());
        long utcTime = OffsetDateTime.parse(date, dtF1).toInstant().toEpochMilli();
        String dateString = dtF1.format(Instant.ofEpochMilli(utcTime));
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(String.valueOf(c.getVehicleId())))
                .latitude(c.getLocation().getLatitude())
                .longitude(c.getLocation().getLongitude())
                .speed(c.getSpeed())
                .direction(c.getBearing() != null ? c.getBearing().doubleValue() : null)
                .dateString(dateString)
                .dateFormat(dF1)
                .metadata(c.getMetadata())
                .utcTimeValue(utcTime)
                .ignition(c.getIgnition())
                .source(SourceMapping.CARTRACK)
                .batteryLevel(c.getTcuBatteryPercentage())
                .accuracy(c.getLocation().getGpsFixType() != null ?
                        (c.getLocation().getGpsFixType() / 3.0) * 100 : 0.0);
    }

    /**
     * Converts a {@link CartrackPositionRaw} into a finalized {@link RawLiveposData}.
     *
     * @param c the CartrackPositionRaw instance to convert
     * @return a {@link RawLiveposData} object built from the given CartrackPositionRaw
     */
    public static RawLiveposData fromCartrackForEvent(CartrackPositionRaw c) {
        return new RawLiveposData.Builder(fromCartrack(c))
                .deviceId(Utils.cleanSerial(String.valueOf(c.getVehicleId())))
                .created(OffsetDateTime.parse(Utils.normalizeDateOffset(c.getEventTs())
                        , dtF1).toInstant().getEpochSecond())
                .dischargeAir(c.getTemp1())
                .returnAir(c.getTemp2())
                .build();
    }

    // --- VOLVO MAPPERS ---

    /**
     * Converts a {@link VolvoLiveposRaw} into a {@link RawTripData.Builder}.
     *
     * @param v the VolvoLiveposRaw instance to convert
     * @return a {@link RawTripData.Builder} initialized with data from the VolvoLiveposRaw
     * @throws ParseException if the date parsing fails
     */
    public static RawTripData.Builder fromVolvo(VolvoLiveposRaw v) throws ParseException {
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(v.getVin()))
                .latitude(v.getGnssPosition().getLatitude())
                .longitude(v.getGnssPosition().getLongitude())
                .speed(v.getWheelBasedSpeed())
                .direction(v.getGnssPosition().getHeading() != null ?
                        v.getGnssPosition().getHeading().doubleValue() : null)
                .dateString(v.getCreatedDateTime())
                .dateFormat(dF2)
                .metadata(v.getMetadata())
                .utcTimeValue(TripProcessingUtils.returnTimeInSeconds(v.getCreatedDateTime(), 1, dF2))
                .ignitionString(v.getTriggerType().getTriggerType())
                .source(SourceMapping.VOLVO);
    }

    /**
     * Converts a {@link VolvoLiveposRaw} into a finalized {@link RawLiveposData}.
     *
     * @param v the VolvoLiveposRaw instance to convert
     * @return a {@link RawLiveposData} object built from the given VolvoLiveposRaw
     * @throws ParseException if the date parsing fails
     */
    public static RawLiveposData fromVolvoForEvent(VolvoLiveposRaw v) throws ParseException {
        return new RawLiveposData.Builder(fromVolvo(v))
                .deviceId(Utils.cleanSerial(v.getVin()))
                .created(TripProcessingUtils.returnTimeInSeconds(v.getCreatedDateTime(), 1000, dF2))
                .build();
    }

    // --- THERMOKING MAPPERS ---

    /**
     * Converts a {@link ThermokingData} object into a {@link RawTripData.Builder}.
     *
     * @param t the ThermokingData instance to convert
     * @return a {@link RawTripData.Builder} initialized with data from the ThermokingData
     */
    public static RawTripData.Builder fromThermoking(ThermokingData t) {
        String dateString = t.getDataDate();
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(t.getReeferSerialNumber()))
                .latitude(t.getLatitude())
                .longitude(t.getLongitude())
                .speed(t.getSpeed())
                .dateString(dateString)
                .dateFormat(dF4)
                .metadata(t.getTmMetadata())
                .ignition(t.getIgnitionStatus().equals("ON"))
                .source(SourceMapping.THERMOKING);
    }

    /**
     * Converts a {@link ThermokingData} object into a finalized {@link RawLiveposData} for event processing.
     *
     * @param t the ThermokingData instance to convert
     * @return a {@link RawLiveposData} object built from the ThermokingData
     */
    public static RawLiveposData fromThermokingForEvent(ThermokingData t) {
        return new RawLiveposData.Builder(fromThermoking(t))
                .deviceId(Utils.cleanSerial(t.getReeferSerialNumber()))
                .created(Utils.getCreatedTimeFromThermokingData(t))
                .dischargeAir(t.getDischargeAir1())
                .returnAir(t.getReturnAir1())
                .mainBatteryLevel(t.getVoltage())
                .setPoint1(t.getSetPoint1())
                .setPoint2(t.getSetPoint2())
                .setPoint3(t.getSetPoint3())
                .zone1DoorOpen(t.isZone1DoorOpen())
                .zone2DoorOpen(t.isZone2DoorOpen())
                .zone3DoorOpen(t.isZone3DoorOpen())
                .fuelLevel(t.getFuelLevel() * 1.0)
                .fuelTankSize(t.getFuelTankSize())
                .build();
    }

    // --- LYNX MAPPERS ---

    /**
     * Converts a {@link LynxRawData} object into a {@link RawTripData.Builder}.
     *
     * @param l value for LynxRawData
     * @return a {@link RawTripData.Builder} initialized from LynxRawData
     */
    public static RawTripData.Builder fromLynx(LynxRawData l) {
        String dateString = l.getEventDateTimeUTC();
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(l.getAssetInfo().getAssetId()))
                .latitude(Double.parseDouble(l.getPositionInfo().getLat()))
                .longitude(Double.parseDouble(l.getPositionInfo().getLon()))
                .speed(l.getStatusInfo().getPositionSpeed())
                .dateString(dateString)
                .dateFormat(dF2)
                .metadata(l.getMetadata())
                .ignition(l.getAssetInfo().getStatus() != null)
                .source(SourceMapping.LYNX);
    }

    /**
     * Converts a {@link LynxRawData} object into a finalized {@link RawLiveposData}.
     *
     * @param l value for LynxRawData
     * @return a {@link RawLiveposData} initialized from LynxRawData
     */
    public static RawLiveposData fromLynxForEvent(LynxRawData l) {
        return new RawLiveposData.Builder(fromLynx(l))
                .deviceId(l.getAssetInfo().getTruSerialNumber())
                .setPoint1(l.getTemperatureInfo().getSetpointTemp1())
                .setPoint2(l.getTemperatureInfo().getSetpointTemp2())
                .setPoint3(l.getTemperatureInfo().getSetpointTemp3())
                .rearDoorOpen(l.getStatusInfo().getRearDoorOpen())
                .sideDoorOpen(l.getStatusInfo().getSideDoorOpen())
                .fuelLevel(l.getStatusInfo().getFuelLevel())
                .fuelTankSize(100)
                .build();
    }

    // --- RMS MAPPERS ---

    /**
     * Converts a {@link RmsLiveposRaw} object into a {@link RawTripData.Builder}.
     *
     * @param r value for RmsLiveposRaw
     * @return a {@link RawTripData.Builder} initialized from RmsLiveposRaw
     */
    public static RawTripData.Builder fromRms(RmsLiveposRaw r) {
        String dateString = r.getGpsDate();
        return new RawTripData.Builder()
                .serial(Utils.cleanSerial(r.getVehiclePk()))
                .latitude(Double.parseDouble(r.getLatitude()))
                .longitude(Double.parseDouble(r.getLongitude()))
                .speed(Double.parseDouble(r.getSpeed()))
                .direction(Double.parseDouble(r.getHeading()))
                .dateString(dateString)
                .dateFormat(dF1)
                .metadata(r.getMetadata())
                .source(SourceMapping.RMS);
    }

    /**
     * Converts a {@link RmsLiveposRaw} object into a finalized {@link RmsLiveposRaw}.
     *
     * @param r value for RmsLiveposRaw
     * @return a {@link RawLiveposData} initialized from RmsLiveposRaw
     */
    public static RawLiveposData fromRmsForEvent(RmsLiveposRaw r) {
        return new RawLiveposData.Builder(fromRms(r))
                .deviceId(r.getVehiclePk())
                .build();
    }
}
