package za.co.trackmatic.flink.models.paragon;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * Represents a single data record reported by a Paragon telematics device.
 * This includes information such as GPS data, ADC readings, panic status, temperatures,
 * ignition state, cellular signal, and more.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataItem implements Serializable {

    public DataItem() {

    }

    private String ver;

    private Gps gps;

    private double acc;

    private Object logs;

    private ADC adc;

    private Integer pr;

    private Integer ig;

    private Long tid;

    private Integer ds;

    private Integer ds2;

    private long tnum;

    private Integer la;

    private Integer tseq;

    private String psdt;

    private Double t1;

    private Double t2;

    private String csq;

    private String mt;

    private Cdd cdd;

    private Integer pa;

    private Object panic;

    private String sl;

    private Integer rfb;

    private String did;

    /**
     * Inner class representing analog-to-digital converter (ADC) readings.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ADC implements Serializable {
        private double a1;

        private double a2;

        private double a3;

        private double a4;

        public double getA1() {
            return a1;
        }

        public void setA1(double a1) {
            this.a1 = a1;
        }

        public double getA2() {
            return a2;
        }

        public void setA2(double a2) {
            this.a2 = a2;
        }

        public double getA3() {
            return a3;
        }

        public void setA3(double a3) {
            this.a3 = a3;
        }

        public double getA4() {
            return a4;
        }

        public void setA4(double a4) {
            this.a4 = a4;
        }
    }

    /**
     * Inner class representing crash detection details, including acceleration vectors.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Cdd implements Serializable{
        private long id;

        private long secs;

        private Integer usecs;

        private Integer seq;

        private double ax;

        private double ay;

        private double az;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getSecs() {
            return secs;
        }

        public void setSecs(long secs) {
            this.secs = secs;
        }

        public Integer getUsecs() {
            return usecs;
        }

        public void setUsecs(Integer usecs) {
            this.usecs = usecs;
        }

        public Integer getSeq() {
            return seq;
        }

        public void setSeq(Integer seq) {
            this.seq = seq;
        }

        public double getAx() {
            return ax;
        }

        public void setAx(double ax) {
            this.ax = ax;
        }

        public double getAy() {
            return ay;
        }

        public void setAy(double ay) {
            this.ay = ay;
        }

        public double getAz() {
            return az;
        }

        public void setAz(double az) {
            this.az = az;
        }
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public Gps getGps() {
        return gps;
    }

    public void setGps(Gps gps) {
        this.gps = gps;
    }

    public double getAcc() {
        return acc;
    }

    public void setAcc(double acc) {
        this.acc = acc;
    }

    public ADC getAdc() {
        return adc;
    }

    public void setAdc(ADC adc) {
        this.adc = adc;
    }

    public Integer getPr() {
        return pr;
    }

    public void setPr(Integer pr) {
        this.pr = pr;
    }

    public Integer getIg() {
        return ig;
    }

    public void setIg(Integer ig) {
        this.ig = ig;
    }

    public Long getTid() {
        return tid;
    }

    public void setTid(Long tid) {
        this.tid = tid;
    }

    public Integer getLa() {
        return la;
    }

    public void setLa(Integer la) {
        this.la = la;
    }

    public Integer getTseq() {
        return tseq;
    }

    public void setTseq(Integer tseq) {
        this.tseq = tseq;
    }

    public Integer getDs() {
        return ds;
    }

    public void setDs(Integer ds) {
        this.ds = ds;
    }

    public Integer getDs2() {
        return ds2;
    }

    public void setDs2(Integer ds2) {
        this.ds2 = ds2;
    }

    public long getTnum() {
        return tnum;
    }

    public void setTnum(long tnum) {
        this.tnum = tnum;
    }

    public String getPsdt() {
        return psdt;
    }

    public void setPsdt(String psdt) {
        this.psdt = psdt;
    }

    public Double getT1() {
        return t1;
    }

    public void setT1(Double t1) {
        this.t1 = t1;
    }

    public Double getT2() {
        return t2;
    }

    public void setT2(Double t2) {
        this.t2 = t2;
    }

    public Object getLogs() {
        return logs;
    }

    public void setLogs(Object logs) {
        this.logs = logs;
    }

    public String getCsq() {
        return csq;
    }

    public void setCsq(String csq) {
        this.csq = csq;
    }

    public String getMt() {
        return mt;
    }

    public void setMt(String mt) {
        this.mt = mt;
    }

    public Cdd getCdd() {
        return cdd;
    }

    public void setCdd(Cdd cdd) {
        this.cdd = cdd;
    }

    public Integer getPa() {
        return pa;
    }

    public void setPa(Integer pa) {
        this.pa = pa;
    }

    public Object getPanic() {
        return panic;
    }

    public void setPanic(Object panic) {
        this.panic = panic;
    }

    public String getSl() {
        return sl;
    }

    public void setSl(String sl) {
        this.sl = sl;
    }

    public Integer getRfb() {
        return rfb;
    }

    public void setRfb(Integer rfb) {
        this.rfb = rfb;
    }

    public String getDid() {
        return did;
    }

    public void setDid(String did) {
        this.did = did;
    }
}
