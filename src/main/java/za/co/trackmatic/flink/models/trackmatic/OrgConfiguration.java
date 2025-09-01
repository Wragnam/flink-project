package za.co.trackmatic.flink.models.trackmatic;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents the configuration settings for an organization,
 * including routing violations settings.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrgConfiguration implements Serializable {

    /**
     * The unique identifier of the organization.
     */
    private String orgId;

    /**
     * The routing violations configuration for the organization.
     */
    private RoutingViolations routingViolations;

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public RoutingViolations getRoutingViolations() {
        return routingViolations;
    }

    public void setRoutingViolations(RoutingViolations routingViolations) {
        this.routingViolations = routingViolations;
    }
}
