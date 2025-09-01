package za.co.trackmatic.flink.models;

import org.apache.commons.codec.Charsets;
import org.apache.flink.shaded.guava30.com.google.common.io.Resources;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URL;

/**
 * Manages the loading and access of EventMapping configuration
 * from a JSON file located in the classpath under a source-specific folder.
 */
public class EventMappingManager {

    /**
     * The loaded event mapping configuration.
     */
    private transient EventMapping mapping;

    /**
     * Constructs an EventMappingManager and loads the mapping file
     * for the given source.
     *
     * @param source the source name (folder name) to load the mapping from
     * @throws Exception if the mapping file is malformed or cannot be read
     */
    public EventMappingManager(String source) throws Exception {
        readEventMappingFile(source);
    }

    /**
     * Reads the EventMapping JSON file from the classpath under
     * the folder named after the source (lowercased) and parses it.
     *
     * @param source the source folder name
     * @throws Exception if the file is missing, malformed, or unreadable
     */
    private void readEventMappingFile(String source) throws Exception {
        URL url = Resources.getResource(source.toLowerCase()+"/EventMapping.json");
        ObjectMapper om = new ObjectMapper();
        String json = Resources.toString(url, Charsets.UTF_8);
        mapping = om.readValue(json, EventMapping.class);
        if(mapping == null || mapping.getEvents()==null){
            throw new Exception("Malformed Mapping file");
        }

    }

    public EventMapping getMapping() {
        return mapping;
    }

    public void setMapping(EventMapping mapping) {
        this.mapping = mapping;
    }
}
