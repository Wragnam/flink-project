package za.co.trackmatic.flink.models.paragon;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents GPS data captured from a Paragon device.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Gps implements Serializable {

    private String dt;

    private Integer fi;

    private String la;

    private String lo;

    private double co;

    private double sp;

    private Integer ns;

    private double hd;

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public Integer getFi() {
        return fi;
    }

    public void setFi(Integer fi) {
        this.fi = fi;
    }

    public String getLa() {
        return la;
    }

    public void setLa(String la) {
        this.la = la;
    }

    public String getLo() {
        return lo;
    }

    public void setLo(String lo) {
        this.lo = lo;
    }

    public double getCo() {
        return co;
    }

    public void setCo(double co) {
        this.co = co;
    }

    public double getSp() {
        return sp;
    }

    public void setSp(double sp) {
        this.sp = sp;
    }

    public Integer getNs() {
        return ns;
    }

    public void setNs(Integer ns) {
        this.ns = ns;
    }

    public double getHd() {
        return hd;
    }

    public void setHd(double hd) {
        this.hd = hd;
    }

    @Override
    public String toString() {
        return "Gps{" +
                "dt='" + dt + '\'' +
                ", fi=" + fi +
                ", la='" + la + '\'' +
                ", lo='" + lo + '\'' +
                ", co=" + co +
                ", sp=" + sp +
                ", ns=" + ns +
                ", hd=" + hd +
                '}';
    }
}
