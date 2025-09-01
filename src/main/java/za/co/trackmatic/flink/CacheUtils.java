package za.co.trackmatic.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.datacache.PointInGeofenceRequest;
import za.co.trackmatic.flink.models.datacache.PointInGeofenceResponse;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmDevices;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class, has all functions to do with the datacache
 */
public class CacheUtils {

    private static ObjectMapper om = new ObjectMapper();

    /**
     * Retrieves geofence data for a specific organization from the server.
     *
     * @param server The server URL to query.
     * @param orgId The organization ID for which geofences are to be fetched.
     * @return A list of `GeofenceItem` objects or null if the request fails.
     */
    public static List<GeofenceItem> getGeofencesForOrg(String server, String orgId) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            // Send GET request to fetch geofences for the given organization
            HttpGet req = new HttpGet(server + "/getGeoByOrg/" + orgId);
            try (CloseableHttpResponse resp = client.execute(req)){
                if (resp.getStatusLine().getStatusCode() != 200) {
                    return null;
                }

                try (InputStream is = resp.getEntity().getContent()) {
                    GeofenceItem[] items = om.readValue(is, GeofenceItem[].class);
                    return items == null ? null : Arrays.asList(items);
                } finally {
                    EntityUtils.consumeQuietly(resp.getEntity());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Checks if a point is inside a geofence by making a request to the server.
     *
     * @param server The server URL to query.
     * @param orgId The organization ID to check against.
     * @param point The geographical point to check.
     * @param siteIds A list of site IDs to include in the check.
     * @return A `GeofenceItem` object if the point is inside a geofence, or null if not.
     */
    public static GeofenceItem isPointInGeofence(String server, String orgId, LatLong point, List<String> siteIds) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            // Prepare the request body with point coordinates, organization ID, and site IDs
            PointInGeofenceRequest reqBody = new PointInGeofenceRequest();
            reqBody.setLatitude(point.getLat());
            reqBody.setLongitude(point.getLng());
            reqBody.setOrgId(orgId);
            reqBody.setSiteIds(siteIds);
            String reqBodyString = om.writeValueAsString(reqBody);

            // Send POST request to check if the point is in a geofence
            HttpPost req = new HttpPost(server + "/pointInGeofence");
            req.setEntity(new StringEntity(reqBodyString));

           try( CloseableHttpResponse resp = client.execute(req)) {
               if (resp.getStatusLine().getStatusCode() != 200) {
                   return null;
               }

               // Parse the response body into PointInGeofenceResponse
               try (InputStream is = resp.getEntity().getContent()) {
                   PointInGeofenceResponse respBody = om.readValue(is, PointInGeofenceResponse.class);
                   if (respBody == null) {
                       throw new Exception("null response body received");
                   }

                   // Return null if the point is not inside any geofence
                   if (!respBody.isInGeofence()) {
                       return null;
                   }

                   return respBody.getGeofence();
               } finally {
                   EntityUtils.consumeQuietly(resp.getEntity());
               }
           }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Checks if a point is inside any geofence, returning a list of geofences if true.
     *
     * @param server The server URL to query.
     * @param orgId The organization ID to check against.
     * @param point The geographical point to check.
     * @param siteIds A list of site IDs to include in the check.
     * @return A list of `GeofenceItem` objects if the point is inside any geofences, or null if not.
     */
    public static List<GeofenceItem> isPointInGeofences(String server, String orgId, LatLong point, List<String> siteIds) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            // Prepare the request body with point coordinates, organization ID, and site IDs
            PointInGeofenceRequest reqBod = new PointInGeofenceRequest();
            reqBod.setSiteIds(siteIds);
            reqBod.setLatitude(point.getLat());
            reqBod.setLongitude(point.getLng());
            reqBod.setOrgId(orgId);
            String reqBodyString = om.writeValueAsString(reqBod);

            // Send POST request to check if the point is in any geofence
            HttpPost req = new HttpPost(server+"/pointInGeofences");
            req.setEntity(new StringEntity(reqBodyString));

            try (CloseableHttpResponse resp = client.execute(req)) {
                if (resp.getStatusLine().getStatusCode() != 200) {
                    return null;
                }

                // Parse the response body into PointInGeofenceResponse
                try (InputStream is = resp.getEntity().getContent()) {
                    PointInGeofenceResponse respBody = om.readValue(is, PointInGeofenceResponse.class);
                    if (respBody == null) {
                        throw new Exception("null response body received");
                    }


                    // Return null if the point is not inside any geofence
                    if (respBody.getGeofences() == null || respBody.getGeofences().isEmpty() || !respBody.isInGeofence()) {
                        return null;
                    }

                    return respBody.getGeofences();

                } finally {
                    EntityUtils.consumeQuietly(resp.getEntity());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * Retrieves device information from the cache for a given serial number.
     *
     * @param server The server URL to query.
     * @param serial The device serial number.
     * @return The `TmDevice` object corresponding to the serial number, or null if not found.
     */
    public static TmDevices.TmDevice getDeviceFromCache(String server, String serial) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            // Send GET request to retrieve device data for the given serial number
            HttpGet req = new HttpGet(server + "/getDevice/" + serial);
            try(CloseableHttpResponse resp = client.execute(req)) {
                if (resp.getStatusLine().getStatusCode() != 200) {
                    return null;
                }

                // Parse the response body into a TmDevice object
                try (InputStream is = resp.getEntity().getContent()) {
                    return om.readValue(is, TmDevices.TmDevice.class);
                } finally {
                    EntityUtils.consumeQuietly(resp.getEntity());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Checks if the geofence data cache is ready on the server.
     *
     * @param server The server URL to query.
     * @return True if the server is ready and contains geofence data, false otherwise.
     */
    public static boolean isHaveGeofences(String server) {
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            // Send GET request to check if geofence data is ready on the server
            HttpGet req = new HttpGet(server + "/ready");
            try(CloseableHttpResponse resp = client.execute(req)){
            return resp.getStatusLine().getStatusCode() == 200;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Waits for the geofence setup to complete on the server.
     * This method will block until the server indicates it is ready.
     *
     * @param config The configuration containing the server URL.
     */
    public static void waitForSetupComplete(Config config) {
        String server = config.getDatacacheServer();
        while (!CacheUtils.isHaveGeofences(server)) {
            // Sleep for 10 seconds before re-checking
            Common.sleep(10);
        }
    }
}
