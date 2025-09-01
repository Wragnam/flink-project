package za.co.trackmatic.flink.models.udtrucks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents pagination information for paged data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Pagination implements Serializable {

    /** Maximum number of items per page. */
    private Integer limit;

    /** The starting index of the current page within the total result set. */
    private Integer offset;

    /** Total number of items available across all pages. */
    private Integer totalCount;

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }
}
