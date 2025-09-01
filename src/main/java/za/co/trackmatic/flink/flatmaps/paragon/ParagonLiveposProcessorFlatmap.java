package za.co.trackmatic.flink.flatmaps.paragon;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Utils.ParagonUtils;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.DataItem;
import za.co.trackmatic.flink.models.paragon.ParagonLiveposState;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.trackmatic.ActiveLoad;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Flink RichFlatMapFunction that processes raw Paragon live position data ({@link ParagonRawData})
 * and emits enriched {@link GeneralLocation} events.
 * <p>
 * This processor filters for valid GPS data items, parses and converts the location and sensor
 * information, enriches the event with metadata and geofence information, and manages per-device
 * live position state with TTL-enabled keyed state to efficiently handle updates and detect status changes.
 * </p>
 * <p>
 * It extracts the latest valid GPS data from the incoming raw data, computes additional event fields such as
 * temperature readings and location accuracy, updates geofence status and device status, and collects the
 * enriched event for downstream processing.
 * </p>
 */
public class ParagonLiveposProcessorFlatmap extends RichFlatMapFunction<ParagonRawData, GeneralLocation> implements Serializable {

    private Config config;


    public ParagonLiveposProcessorFlatmap(Config config) {
        this.config = config;
    }

    private transient MapState<String, ParagonLiveposState> mapState;

    private static final String dateFormatConverted = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final String dateFormatOriginal = "yyyy-MM-dd HH:mm:ss";

    private static final String dataSource = SourceMapping.PARAGON;


    SimpleDateFormat dateParser = new SimpleDateFormat(dateFormatOriginal);

    /**
     * Initializes the operator by setting up the state descriptor and configuring the time-to-live (TTL)
     * settings for the map state that holds the `ParagonLiveposState` for each device serial.
     * <p>
     * This method defines the map state for storing the state of each device (using the serial as the key),
     * and configures the TTL settings so that the state expires after a certain duration. The state is cleaned
     * up incrementally to avoid memory bloat.
     * </p>
     *
     * @param parameters The configuration parameters for the operator.
     * @throws Exception If an error occurs during the state initialization.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, ParagonLiveposState> descriptor = new MapStateDescriptor<>("orgDeviceLiveposStates", String.class, ParagonLiveposState.class);

        // Configure the time-to-live (TTL) for the map state, where the state expires after 10 hours
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10000, true)
                .build();

        // Enable the TTL configuration on the state descriptor
        descriptor.enableTimeToLive(ttlConfig);

        // Get the map state from the runtime context using the configured descriptor
        mapState = getRuntimeContext().getMapState(descriptor);
    }


    @Override
    public void flatMap(ParagonRawData rawPositionData, Collector<GeneralLocation> collector) throws Exception {
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
            return;
        }

        String serial = rawPositionData.getSerial();
        TmMetadata metadata = rawPositionData.getMetadata();

        ParagonLiveposState state = new ParagonLiveposState();
        if (mapState.contains(serial) && mapState.get(serial) != null) {
            state = mapState.get(serial);
        } else {
            mapState.put(serial, state);
        }

        ActiveLoad load = rawPositionData.getMetadata().getActiveLoad();

        // Update the rawPositionData with the filtered valid data
        rawPositionData.setData(newList);

        // Extract the last valid position data (first in the list after filtering)
        DataItem lastLivepos = rawPositionData.getData().get(0);

        String model = rawPositionData.getSver();
        String dateString = lastLivepos.getGps().getDt();

        // Parse latitude and longitude from the GPS data and set it in the event
        double lat = Utils.parseLatitude(lastLivepos.getGps().getLa());
        double lon = Utils.parseLongitude(lastLivepos.getGps().getLo());
        Date date = dateParser.parse(dateString);
        double speed = lastLivepos.getGps().getSp();
        double direction = lastLivepos.getGps().getCo();
        double accuracy = lastLivepos.getGps().getNs() != null ? (lastLivepos.getGps().getNs().doubleValue() / 12) * 100 : 0;

        GeneralLocation generalPosition = new GeneralLocation(lat, lon, date.getTime() / 1000,
                dataSource, metadata, serial, serial);

        if (load != null) generalPosition.setDriverName(load.getDriverName());

        generalPosition.setLatitude(lat);
        generalPosition.setLongitude(lon);
        generalPosition.setDischargeAir(ParagonUtils.convertVoltageToDegreesCelsius(lastLivepos.getT1(), model));
        generalPosition.setReturnAir(ParagonUtils.convertVoltageToDegreesCelsius(lastLivepos.getT2(), model));
        generalPosition.setSpeed(speed);
        generalPosition.setDirection(direction);
        generalPosition.setAccuracy(accuracy);

        // Get geofence information by checking if the point is inside any geofence
        String server = config.getDatacacheServer();
        GeofenceItem fence = CacheUtils.isPointInGeofence(server, rawPositionData.getMetadata().getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

        Utils.setLocationNameAndId(fence, generalPosition);

        // Determine the status based on GPS speed and set it in the event
        String status = Utils.getStatus(speed);
        generalPosition.setStatus(status);

        if (state.getLastStatus() == null || !Objects.equals(state.getLastStatus(), status)) {
            state.setLastStatus(status);
            state.setLastStatusChangedDate(Utils.convertDate(dateString, dateFormatOriginal, dateFormatConverted));
            mapState.put(serial, state);
        }

        generalPosition.setLastStatusChange(state.getLastStatusChangedDate());

        // Collect the enriched live position event for output
        collector.collect(generalPosition);
    }
}
