package za.co.trackmatic.flink;

import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.processors.blackberry.BlackberryEventEnricher;
import za.co.trackmatic.flink.processors.blackberry.BlackberryEventProcessor;
import za.co.trackmatic.flink.processors.blackberry.BlackberryLiveposEnricher;
import za.co.trackmatic.flink.processors.blackberry.BlackberryLiveposProcessor;
import za.co.trackmatic.flink.processors.cartrack.*;
import za.co.trackmatic.flink.processors.fleetboard.*;
import za.co.trackmatic.flink.processors.icam.IcamEventProcessor;
import za.co.trackmatic.flink.processors.icam.IcamLiveposEnricher;
import za.co.trackmatic.flink.processors.icam.IcamLiveposProcessor;
import za.co.trackmatic.flink.processors.icam.IcamTripProcessor;
import za.co.trackmatic.flink.processors.loads.LoadsUpdateStopProcessor;
import za.co.trackmatic.flink.processors.lynx.LynxEventProcessor;
import za.co.trackmatic.flink.processors.lynx.LynxLiveposProcessor;
import za.co.trackmatic.flink.processors.lynx.LynxRawDataEnricher;
import za.co.trackmatic.flink.processors.lynx.LynxTemperatureProcessor;
import za.co.trackmatic.flink.processors.lytx.LytxEventEnricher;
import za.co.trackmatic.flink.processors.lytx.LytxEventProcessor;
import za.co.trackmatic.flink.processors.mix.MixEventEnricher;
import za.co.trackmatic.flink.processors.mix.MixEventProcessor;
import za.co.trackmatic.flink.processors.mix.MixLiveposEnricher;
import za.co.trackmatic.flink.processors.mix.MixLiveposProcessor;
import za.co.trackmatic.flink.processors.paragon.*;
import za.co.trackmatic.flink.processors.rms.RmsLiveposEnricher;
import za.co.trackmatic.flink.processors.rms.RmsLiveposEventProcessor;
import za.co.trackmatic.flink.processors.rms.RmsTripProcessor;
import za.co.trackmatic.flink.processors.scania.*;
import za.co.trackmatic.flink.processors.surfsight.*;
import za.co.trackmatic.flink.processors.thermoking.*;
import za.co.trackmatic.flink.processors.udtrucks.UdEventEnricher;
import za.co.trackmatic.flink.processors.udtrucks.UdEventProcessor;
import za.co.trackmatic.flink.processors.udtrucks.UdLiveposEnricher;
import za.co.trackmatic.flink.processors.udtrucks.UdLivePosProcessor;
import za.co.trackmatic.flink.processors.volvo.VolvoEventProcessor;
import za.co.trackmatic.flink.processors.volvo.VolvoLiveposEnricher;
import za.co.trackmatic.flink.processors.volvo.VolvoLiveposProcessor;
import za.co.trackmatic.flink.processors.volvo.VolvoTripProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point for the Trackmatic Flink job processing.
 * Based on the configuration provided in the 'config_dir', this class initializes and processes various types of data
 * depending on the processor type specified in the config.
 */
public class Main {

    /**
     * The base directory where configuration files and other resources are located.
     */
    public static String baseDir = "";

    /**
     * Main method to execute the Flink job based on the configuration passed.
     *
     * @param args The command-line arguments (should contain the configuration directory).
     * @throws Exception If an error occurs during processing.
     */
    public static void main(String[] args) throws Exception {
        // Validate the command-line argument for configuration directory.
        if (args.length != 1) {
            System.out.println("Usage: Main config_dir");
            System.exit(1);
        }

        // Set the base directory from command-line argument.
        baseDir = args[0];

        // Load configuration from the specified directory.
        Config config = new ConfigManager(baseDir).getConfig();

        // Process data based on the processor type specified in the configuration.
        switch (config.getProcessorType()) {
            case "tk-enrich": {
                System.out.println("Starting thermoking data enricher");
                //wait until we're ready to start processing points. e.g.
                //only start processing once we have geofences.
                CacheUtils.waitForSetupComplete(config);

                new TkEnricher().process(config);
            }
            break;

            case "tk-event": {
                System.out.println("Starting thermoking event processor");
                new TkRaw().process(config);
            }
            break;

            case "tk-temp": {
                System.out.println("Starting thermoking temp processor");
                new TkTemperature().process(config);
            }
            break;

            case "tk-livepos": {
                System.out.println("Starting thermoking live position processor");
                new TkLivePos().process(config);
            }
            break;

            case "ud-enrich": {
                System.out.println("Starting ud-livepos enricher");
                CacheUtils.waitForSetupComplete(config);

                new UdLiveposEnricher().process(config);
            }
            break;

            case "ud-livepos": {
                System.out.println("Starting ud-livepos processor");
                new UdLivePosProcessor().process(config);
            }
            break;

            case "ud-event-enrich": {
                System.out.println("Starting ud event enricher");
                new UdEventEnricher().process(config);
            }
            break;

            case "ud-event": {
                System.out.println("Starting ud event processor");
                new UdEventProcessor().process(config);
            }
            break;

            case "ss-enrich": {
                System.out.println("Starting surfsight livepos enricher");
                CacheUtils.waitForSetupComplete(config);

                new SurfsightLiveposEnricher().process(config);
            }
            break;

            case "ss-livepos": {
                System.out.println("Starting surfsight livepos");
                new SurfsightLivePosProcessor().process(config);
            }
            break;

            case "ss-event-enrich": {
                System.out.println("Starting surfsight event enricher");
                new SurfsightEventEnricher().process(config);
            }
            break;

            case "ss-event": {
                System.out.println("Starting surfsight event processor");
                new SurfsightEventProcessor().process(config);
            }
            break;

            case "bb-event-enrich": {
                System.out.println("Starting blackberry event enricher");
                new BlackberryEventEnricher().process(config);
            }
            break;

            case "bb-event": {
                System.out.println("Starting blackberry event processor");
                new BlackberryEventProcessor().process(config);
            }
            break;

            case "bb-livepos-enrich": {
                System.out.println("Starting blackberry livepos enricher");
                new BlackberryLiveposEnricher().process(config);
            }
            break;

            case "bb-livepos": {
                System.out.println("Starting blackberry livepos");
                new BlackberryLiveposProcessor().process(config);
            }
            break;

            case "fb-fuel": {
                System.out.println("Starting fleetboard fuel confirmation processor");
                new FleetboardFuelVerifierProcessor().process(config);
            }
            break;

            case "fb-event-enrich": {
                System.out.println("Starting fleetboard event enricher");
                new FleetboardEventEnricher().process(config);
            }
            break;

            case "fb-event": {
                System.out.println("Starting fleetboard event processor");
                new FleetboardEventProcessor().process(config);
            }
            break;

            case "fb-enrich": {
                System.out.println("Starting fleetboard livepos enricher");
                new FleetboardLiveposEnricher().process(config);
            }
            break;

            case "fb-livepos": {
                System.out.println("Starting fleetboard livepos processor");
                new FleetboardLiveposProcessor().process(config);
            }
            break;

            case "lytx-event-enrich": {
                System.out.println("Starting lytx event enricher");
                new LytxEventEnricher().process(config);
            }
            break;

            case "lytx-event": {
                System.out.println("Starting lytx event processor");
                new LytxEventProcessor().process(config);
            }
            break;

//            case "lytx-livepos-enrich":{
//                System.out.println("Starting lytx livepos enricher");
//                new LytxLiveposEnricher().process(config);
//            }
//            break;
//
//            case "lytx-livepos":{
//                System.out.println("Starting lytx livepos processor");
//                new LytxLiveposProcessor().process(config);
//            }
//            break;

            case "pg-livepos-enrich": {
                System.out.println("Starting paragon livepos enricher");
                new ParagonLiveposEnricher().process(config);
            }
            break;

            case "pg-livepos": {
                System.out.println("Starting paragon livepos processor");
                new ParagonLiveposProcessor().process(config);
            }
            break;

            case "pg-event-enrich": {
                System.out.println("Starting paragon event enricher");
                new ParagonEventEnricher().process(config);
            }
            break;

            case "pg-event": {
                System.out.println("Starting paragon event processor");
                new ParagonEventProcessor().process(config);
            }
            break;

            case "pg-trip-process": {
                System.out.println("Starting paragon trip processor");
                new ParagonTripProcessor().process(config);
            }
            break;

            case "pg-temp-livepos-process": {
                System.out.println("Starting paragon temp/livepos processor");
                new ParagonLiveposTemp().process(config);
            }
            break;

            case "pg-temp": {
                System.out.println("Starting paragon temp processor");
                new ParagonTemperature().process(config);
            }
            break;

            case "icam-livepos-enrich": {
                System.out.println("Starting icam livepos enricher");
                new IcamLiveposEnricher().process(config);
            }
            break;

            case "icam-livepos": {
                System.out.println("Starting icam livepos processor");
                new IcamLiveposProcessor().process(config);
            }
            break;

            case "icam-event": {
                System.out.println("Starting icam event processor");
                new IcamEventProcessor().process(config);
            }
            break;

            case "icam-trip-process": {
                System.out.println("Starting icam trip processor");
                new IcamTripProcessor().process(config);
            }
            break;

            case "mix-event-enrich": {
                System.out.println("Starting mix event enricher");
                new MixEventEnricher().process(config);
            }
            break;

            case "mix-event": {
                System.out.println("Starting mix event processor");
                new MixEventProcessor().process(config);
            }
            break;

            case "mix-livepos-enrich": {
                System.out.println("Starting mix livepos enricher");
                new MixLiveposEnricher().process(config);
            }
            break;

            case "mix-livepos": {
                System.out.println("Starting mix livepos processor");
                new MixLiveposProcessor().process(config);
            }
            break;

            case "lynx-rawdata-enrich": {
                System.out.println("Starting lynx data enricher");
                new LynxRawDataEnricher().process(config);
            }
            break;

            case "lynx-event": {
                System.out.println("Starting lynx event processor");
                new LynxEventProcessor().process(config);
            }
            break;

            case "lynx-livepos": {
                System.out.println("Starting lynx livepos processor");
                new LynxLiveposProcessor().process(config);
            }
            break;

            case "lynx-temp": {
                System.out.println("Starting lynx temperature processor");
                new LynxTemperatureProcessor().process(config);
            }
            break;

            case "volvo-livepos": {
                System.out.println("Starting volvo livepos processor");
                new VolvoLiveposProcessor().process(config);
            }
            break;

            case "volvo-livepos-enrich": {
                System.out.println("Starting volvo livepos enricher");
                new VolvoLiveposEnricher().process(config);
            }
            break;

            case "volvo-trip-process": {
                System.out.println("Starting volvo trip processing");
                new VolvoTripProcessor().process(config);
            }
            break;

            case "volvo-event": {
                System.out.println("Starting volvo event processor");
                new VolvoEventProcessor().process(config);
            }
            break;

            case "cartrack-livepos-enrich": {
                System.out.println("Starting cartrack livepos enricher");
                new CartrackLiveposEnricher().process(config);
            }
            break;

            case "cartrack-livepos": {
                System.out.println("Starting cartrack livepos processor");
                new CartrackLiveposProcessor().process(config);
            }
            break;

            case "cartrack-event-enrich": {
                System.out.println("Starting cartrack event enricher");
                new CartrackEventEnricher().process(config);
            }
            break;

            case "cartrack-event": {
                System.out.println("Starting cartrack event processor");
                new CartrackEventProcessor().process(config);
            }
            break;

            case "cartrack-livepos-event": {
                System.out.println("Starting cartrack livepos-event processor");
                new CartrackLiveposEventProcessor().process(config);
            }
            break;

            case "cartrack-trip-process": {
                System.out.println("Starting cartrack trip processor");
                new CartrackTripProcessor().process(config);
            }
            break;

            case "scania-livepos-enrich": {
                System.out.println("Starting scania livepos enricher");
                new ScaniaLiveposEnricher().process(config);
            }
            break;

            case "scania-livepos": {
                System.out.println("Starting scania livepos processor");
                new ScaniaLiveposProcessor().process(config);
            }
            break;

            case "scania-event": {
                System.out.println("Starting scania event processor");
                new ScaniaEventProcessor().process(config);
            }
            break;

            case "scania-livepos-event": {
                System.out.println("Starting scania livepos event processor");
                new ScaniaLiveposEventProcessor().process(config);
            }
            break;

            case "scania-trip-process": {
                System.out.println("Starting scania trip processor");
                new ScaniaTripProcessor().process(config);
            }
            break;

            case "rms-livepos-enrich" : {
                System.out.println("Starting rms livepos enricher");
                new RmsLiveposEnricher().process(config);
            }
            break;

            case "rms-livepos-event": {
                System.out.println("Starting rms livepos event processor");
                new RmsLiveposEventProcessor().process(config);
            }
            break;

            case "rms-trip-process": {
                System.out.println("Starting rms trip processor");
                new RmsTripProcessor().process(config);
            }
            break;

            case "extrapolate-loads-stop": {
                System.out.println("Starting extrapolation of load stops");
                new LoadsUpdateStopProcessor().process(config);
            }
            break;

            case "tk-logger": {
                System.out.println("Staring tk-logger processor");

                /* get the list of devices to filter */
                List<String> devices = new ArrayList<>();
                try (BufferedReader br = new BufferedReader(new FileReader(new File(baseDir + "/devices")))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.isEmpty()) {
                            continue;
                        }

                        devices.add(line);
                        System.out.println("-> " + line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                new TkLogger().process(config, devices);
            }
            break;

            default: {
                System.out.println("Unknown processor specified");
                System.exit(1);
            }
            break;
        }
    }
}