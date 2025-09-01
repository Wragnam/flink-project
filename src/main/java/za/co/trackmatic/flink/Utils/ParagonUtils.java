package za.co.trackmatic.flink.Utils;

import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.events.paragon.AccidentEvent;
import za.co.trackmatic.flink.events.universal.BatteryEvent;
import za.co.trackmatic.flink.models.MultipleProviders.EventState;
import za.co.trackmatic.flink.models.paragon.DataItem;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.trackmatic.*;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ParagonUtils implements Serializable {

    SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String dataSource = SourceMapping.PARAGON;


    /**
     * Creates and handles a panic event for a `ParagonGeneralEvent`, updating the event's state and
     * notifying control rooms if a panic condition is met.
     *
     * @param mainGeneralEvent The event that potentially represents a panic situation.
     * @param state            The current state of the event, tracking the last panic state and time.
     * @param collector        The collector used to emit the event if a panic is detected.
     * @param metadata         The metadata for setting control rooms.
     */
    public static void createPanicEvent(GeneralEvent mainGeneralEvent, EventState state, Integer panic, Collector<GeneralEvent> collector, TmMetadata metadata) {
        // Check if the panic condition in the data item is set
        if (panic != null && panic == 1) {
            // Only trigger panic if the previous panic state was inactive (0)
            // and more than 30 minutes have passed since the last panic event
            if (state.getPanic() == 0 && mainGeneralEvent.getCreated() - state.getLastPanicTime() > 1800) {

                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);

                generalEvent.setType("PANIC");

                // Update the state to reflect that a panic is now active
                state.setPanic(1);

                // Update the last panic time to the current processed time
                state.setLastPanicTime(generalEvent.getCreated());
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);

                // Collect the updated event to notify downstream processes
                collector.collect(generalEvent);
            }
        } else {
            // Reset panic state to inactive (0) if no panic condition is set
            state.setPanic(0);
        }
    }

    /**
     * Creates and handles an accident event for a `ParagonGeneralEvent`, setting the event details based on accelerometer data
     * and updating the state to reflect the accident occurrence.
     *
     * @param mainGeneralEvent The event that may represent an accident.
     * @param cdd             The data item containing accident-related data from sensors.
     * @param state            The current state of the event, tracking accident occurrences.
     * @param collector        The collector used to emit the event if an accident is detected.
     * @param metadata         The metadata for setting control rooms.
     */
    public static void createAccidentEvent(GeneralEvent mainGeneralEvent, DataItem.Cdd cdd, EventState state, Collector<GeneralEvent> collector, TmMetadata metadata) {
        // Check if accident-related data (`cdd`) is present in the data item
        if (cdd != null) {
            // Trigger an accident event only if there has been no prior recent accident (counter is 0)
            if (state.getAccidentCounter() == 0) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("ACCIDENT");

                // Create a new `AccidentEvent` and populate it with accelerometer data
                AccidentEvent accidentEvent = new AccidentEvent();
                accidentEvent.setAccelerometerX(cdd.getAx());
                accidentEvent.setAccelerometerY(cdd.getAy());
                accidentEvent.setAccelerometerZ(cdd.getAz());

                // Set the event time by summing microseconds and seconds from `cdd`
                accidentEvent.setTime(cdd.getUsecs() + cdd.getSecs());

                generalEvent.setAccidentEvent(accidentEvent);

                // Update the state to reflect that an accident event has been triggered
                state.setAccidentCounter(1);
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);

                // Collect the updated general event to emit it downstream
                collector.collect(generalEvent);
            }
        } else {
            // Reset the accident counter if no accident-related data is detected
            state.setAccidentCounter(0);
        }
    }

    /**
     * Creates and manages battery events for a `ParagonGeneralEvent`, identifying battery disconnections
     * or low voltage levels for main and backup batteries based on voltage readings.
     *
     * @param mainGeneralEvent The general event where battery status changes are recorded.
     * @param collector        The collector used to emit the event if a battery-related status change is detected.
     * @param state            The current state of battery event counters to track main and backup battery conditions.
     * @param metadata         The metadata for setting control rooms.
     */
    public static void createBatteryEvent(GeneralEvent mainGeneralEvent, Double A1, Double A2, Collector<GeneralEvent> collector, EventState state, TmMetadata metadata) {
        // Check if main battery (ADC) is disconnected and hasn't been reported already
        if (A1 == null && A2 == null && state.getMainBatteryEventCounter() != 2) {
            // Set the event type as a battery disconnection for the main battery
            GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
            generalEvent.setType("BATTERY_DISCONNECT");
            generalEvent.setProcessed(System.currentTimeMillis() / 1000);
            BatteryEvent batteryEvent = new BatteryEvent();
            batteryEvent.setBatteryType("MAIN_BATTERY");
            batteryEvent.setVoltage(0.0);

            generalEvent.setBatteryEvent(batteryEvent);

            // Assign control rooms and emit the event
            EventUtils.setControlRooms(generalEvent, metadata, dataSource);
            collector.collect(generalEvent);

            // Update state counters to reflect the main and backup battery disconnection statuses
            state.setMainBatteryEventCounter(2);
            state.setBackupBatteryEventCounter(2);
            return;
        }

        // Skip processing if main battery ADC data is still unavailable
        if (A1 == null && A2 == null) {
            return;
        }

        // Check for main battery disconnection (voltage 0.0) and create event if not already reported
        if (A1 != null && A1 == 0.0) {
            if (state.getMainBatteryEventCounter() != 2) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_DISCONNECT");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("MAIN_BATTERY");
                batteryEvent.setVoltage(0.0);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setMainBatteryEventCounter(2);
                collector.collect(generalEvent);
            }
        }
        // Check if main battery voltage is low (< 11.5) and create event if not reported recently
        else if (A1 != null && A1 < 11.5) {
            if (state.getMainBatteryEventCounter() == 0) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_LOW");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("MAIN_BATTERY");
                batteryEvent.setVoltage(A1);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setMainBatteryEventCounter(1);
                collector.collect(generalEvent);
            }
        }
        // Reset counter if voltage is back to normal
        else {
            state.setMainBatteryEventCounter(0);
        }

        // Check for backup battery disconnection (voltage 0.0) and create event if not already reported
        if (A2 != null && A2 == 0.0) {
            if (state.getBackupBatteryEventCounter() != 2) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_DISCONNECT");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("BACKUP_BATTERY");
                batteryEvent.setVoltage(0);

                generalEvent.setBatteryEvent(batteryEvent);

                // Assign control rooms and emit the event
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setBackupBatteryEventCounter(2);
                collector.collect(generalEvent);
            }
        }
        // Check if backup battery voltage is low (< 3.0) and create event if not reported recently
        else if (A2 != null && A2 < 3.0) {
            if (state.getBackupBatteryEventCounter() == 0) {
                GeneralEvent generalEvent = Utils.deepCopy(mainGeneralEvent);
                generalEvent.setType("BATTERY_LOW");

                BatteryEvent batteryEvent = new BatteryEvent();
                batteryEvent.setBatteryType("BACKUP_BATTERY");
                batteryEvent.setVoltage(A2);

                generalEvent.setBatteryEvent(batteryEvent);
                EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                state.setBackupBatteryEventCounter(1);
                collector.collect(generalEvent);
            }
        }
        // Reset counter if voltage is back to normal
        else {
            state.setBackupBatteryEventCounter(0);
        }
    }

    public DataItem getLocationDataItem(ParagonRawData rawPositionData) {
        // Flag to check if valid information is found
        boolean hasInformation = false;
        List<DataItem> newList = new ArrayList<>();

        // Iterate through the raw position data items and filter for valid GPS data
        for (DataItem item : rawPositionData.getData()) {// If the GPS Fi value is non-zero, it's valid
            if (item.getGps().getFi() != 0) {
                // Add valid item to the list
                newList.add(item);

                // Mark that valid information is found
                hasInformation = true;

                // Process only the first valid item
                break;
            }
        }

        // If no valid information is found, skip this record
        if (!hasInformation) {
            return null;
        }

        // Update the rawPositionData with the filtered valid data
        rawPositionData.setData(newList);

        // Extract and return the last valid position data (first in the list after filtering)
        return rawPositionData.getData().get(0);
    }

    /**
     * Parses a date string in the Paragon-specific format and converts it into a {@link Date} object.
     *
     * @param paragonDate The date string to parse, expected to be in the format understood by the {@code dateParser}.
     * @return A {@link Date} object representing the parsed date and time.
     * @throws ParseException If the input {@code paragonDate} cannot be parsed due to an invalid format or other issues.
     */
    public Date getDateTimeFromParagonDate(String paragonDate) throws ParseException {
        return dateParser.parse(paragonDate);
    }

    public static Double convertVoltageToDegreesCelsius(Double voltage, String model) {
        if (voltage == null) return null;

        double multiplier = (model == null || model.isEmpty() || model.toLowerCase().contains("p")) ? 37.5 : 11;
        double lookup = (multiplier * (voltage / 3.3) - 1) / (1 - (voltage / 3.3));

        return getTemperatureFromLookup(lookup);
    }


    private static final TreeMap<Double, Double> values = new TreeMap<>();

    private static Double getTemperatureFromLookup(double lookup) {
        Double result = null;
        for (Map.Entry<Double, Double> entry : values.entrySet()) {
            if (lookup <= entry.getValue()) {
                result = entry.getKey();
            } else {
                break;
            }
        }
        return result;
    }

    static {
        values.put(-50.0, 329.2);
        values.put(-49.0, 310.7);
        values.put(-48.0, 293.3);
        values.put(-47.0, 277.0);
        values.put(-46.0, 261.8);
        values.put(-45.0, 247.5);
        values.put(-44.0, 234.1);
        values.put(-43.0, 221.6);
        values.put(-42.0, 209.8);
        values.put(-41.0, 198.7);
        values.put(-40.0, 188.4);
        values.put(-39.0, 178.3);
        values.put(-38.0, 168.9);
        values.put(-37.0, 160.1);
        values.put(-36.0, 151.8);
        values.put(-35.0, 144.0);
        values.put(-34.0, 136.6);
        values.put(-33.0, 129.7);
        values.put(-32.0, 123.2);
        values.put(-31.0, 117.1);
        values.put(-30.0, 111.3);
        values.put(-29.0, 105.7);
        values.put(-28.0, 100.4);
        values.put(-27.0, 95.47);
        values.put(-26.0, 90.8);
        values.put(-25.0, 86.39);
        values.put(-24.0, 82.22);
        values.put(-23.0, 78.29);
        values.put(-22.0, 74.58);
        values.put(-21.0, 71.07);
        values.put(-20.0, 67.74);
        values.put(-19.0, 64.54);
        values.put(-18.0, 61.52);
        values.put(-17.0, 58.66);
        values.put(-16.0, 53.39);
        values.put(-15.0, 53.39);
        values.put(-14.0, 50.96);
        values.put(-13.0, 48.65);
        values.put(-12.0, 46.48);
        values.put(-11.0, 44.41);
        values.put(-10.0, 42.25);
        values.put(-9.0, 40.56);
        values.put(-8.0, 38.76);
        values.put(-7.0, 37.05);
        values.put(-6.0, 35.43);
        values.put(-5.0, 33.89);
        values.put(-4.0, 32.43);
        values.put(-3.0, 31.04);
        values.put(-2.0, 29.72);
        values.put(-1.0, 28.47);
        values.put(0.0, 27.28);
        values.put(1.0, 26.13);
        values.put(2.0, 25.03);
        values.put(3.0, 23.99);
        values.put(4.0, 22.99);
        values.put(5.0, 22.05);
        values.put(6.0, 21.15);
        values.put(7.0, 20.29);
        values.put(8.0, 19.4);
        values.put(9.0, 18.7);
        values.put(10.0, 17.96);
        values.put(11.0, 17.24);
        values.put(12.0, 16.55);
        values.put(13.0, 15.9);
        values.put(14.0, 15.28);
        values.put(15.0, 14.68);
        values.put(16.0, 14.12);
        values.put(17.0, 13.57);
        values.put(18.0, 13.06);
        values.put(19.0, 12.56);
        values.put(20.0, 12.09);
        values.put(21.0, 11.63);
        values.put(22.0, 11.2);
        values.put(23.0, 10.78);
        values.put(24.0, 10.38);
        values.put(25.0, 10.0);
        values.put(26.0, 9.63);
        values.put(27.0, 9.28);
        values.put(28.0, 8.94);
        values.put(29.0, 8.62);
        values.put(30.0, 8.31);
        values.put(31.0, 8.01);
        values.put(32.0, 7.72);
        values.put(33.0, 7.45);
        values.put(34.0, 7.19);
        values.put(35.0, 6.94);
        values.put(36.0, 6.69);
        values.put(37.0, 6.46);
        values.put(38.0, 6.24);
        values.put(39.0, 6.03);
        values.put(40.0, 5.82);
        values.put(41.0, 5.63);
        values.put(42.0, 5.43);
        values.put(43.0, 5.25);
        values.put(44.0, 5.08);
        values.put(45.0, 4.91);
        values.put(46.0, 4.74);
        values.put(47.0, 4.59);
        values.put(48.0, 4.44);
        values.put(49.0, 4.3);
        values.put(50.0, 4.16);
        values.put(51.0, 4.02);
        values.put(52.0, 3.9);
        values.put(53.0, 3.77);
        values.put(54.0, 3.65);
        values.put(55.0, 3.53);
        values.put(56.0, 3.42);
        values.put(57.0, 3.31);
        values.put(58.0, 3.21);
        values.put(59.0, 3.11);
        values.put(60.0, 3.02);
        values.put(61.0, 2.92);
        values.put(62.0, 2.83);
        values.put(63.0, 2.75);
        values.put(64.0, 2.66);
        values.put(65.0, 2.58);
        values.put(66.0, 2.51);
        values.put(67.0, 2.43);
        values.put(68.0, 2.36);
        values.put(69.0, 2.29);
        values.put(70.0, 2.22);
        values.put(71.0, 2.16);
        values.put(72.0, 2.1);
        values.put(73.0, 2.04);
        values.put(74.0, 1.98);
        values.put(75.0, 1.92);
        values.put(76.0, 1.87);
        values.put(77.0, 1.81);
        values.put(78.0, 1.76);
        values.put(79.0, 1.71);
        values.put(80.0, 1.66);
        values.put(81.0, 1.62);
        values.put(82.0, 1.57);
        values.put(83.0, 1.53);
        values.put(84.0, 1.49);
        values.put(85.0, 1.45);
        values.put(86.0, 1.41);
        values.put(87.0, 1.37);
        values.put(88.0, 1.33);
        values.put(89.0, 1.3);
        values.put(90.0, 1.26);
        values.put(91.0, 1.23);
        values.put(92.0, 1.2);
        values.put(93.0, 1.16);
        values.put(94.0, 1.13);
        values.put(95.0, 1.1);
        values.put(96.0, 1.08);
        values.put(97.0, 1.05);
        values.put(98.0, 1.02);
        values.put(99.0, 0.99);
        values.put(100.0, 0.97);
        values.put(101.0, 0.94);
        values.put(102.0, 0.92);
        values.put(103.0, 0.9);
        values.put(104.0, 0.87);
        values.put(105.0, 0.85);
        values.put(106.0, 0.83);
        values.put(107.0, 0.81);
        values.put(108.0, 0.79);
        values.put(109.0, 0.77);
        values.put(110.0, 0.75);
    }
}
