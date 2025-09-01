package za.co.trackmatic.flink.flatmaps.paragon;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;

/**
 * A Flink RichFlatMapFunction that processes and enriches incoming {@link ParagonRawData} events.
 * <p>
 * This class enriches raw Paragon data by cleaning and standardizing the device serial number,
 * retrieving associated metadata from the configuration, and attaching it to the event.
 * It also performs filtering to exclude events with unknown or missing metadata.
 * </p>
 * <p>
 * The enriched {@link ParagonRawData} is then emitted downstream for further processing.
 * </p>
 */
public class ParagonEventEnricherFlatmap extends RichFlatMapFunction<ParagonRawData, ParagonRawData> {

    private Config config;

    public ParagonEventEnricherFlatmap(Config config){
        this.config = config;
    }

    /**
     * Processes the incoming `ParagonRawData`, enriches it with metadata, removes duplicate data items,
     * and collects the processed data if it is valid.
     * <p>
     * This method cleans the serial number, retrieves metadata based on the serial, and uses a `TrackingData` object
     * to remove duplicate data items. If the resulting data is not empty, the method collects the enriched `ParagonRawData`.
     * </p>
     *
     * @param rawEventData The `ParagonRawData` object to be processed.
     * @param collector The collector used to emit the processed data.
     * @throws Exception If an error occurs while processing the data.
     */
    @Override
    public void flatMap(ParagonRawData rawEventData, Collector<ParagonRawData> collector) throws Exception {
        // Clean the serial number from the raw event data
        String serial = Utils.cleanSerial(rawEventData.getSerial());
        if(serial.equals(Utils.UNKNOWN)){
            return;
        }

        // Retrieve metadata for the given serial from the configuration
        TmMetadata metadata = Utils.getMetaData(serial,config,"pg-event-enricher");

        if(metadata == null){
            return;
        }

        // Set the retrieved metadata on the raw event data
        rawEventData.setMetadata(metadata);

        // Collect the processed raw event data
        collector.collect(rawEventData);
    }
}
