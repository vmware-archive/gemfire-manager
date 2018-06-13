package io.pivotal.pde.penske.loader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.gemfire.mapping.Region;
@Region("UTLatest")
public class UTLatest implements Serializable {
    private static final long serialVersionUID = 4450606242235200369L;
    @Id
    private String vin;
    private String fileName;
    private String flag;
    private String fuel_level;
    private String odometer;
    private String heading;
    private double latitude;
    private String speed;
    private String read_datetime;
    private String capture_datetime;
    private List<String> warning;
    private String tsp_provider;
    private String id;
    private double longitude;
    public UTLatest (){};

    public UTLatest(String vin, String fileName, String flag, String fuel_level, String odometer, String heading,
                         double latitude, String speed, String read_datetime, String capture_datetime, List<String> warning,
                         String tsp_provider, String id, double longitude) {
        super();
        this.vin = vin;
        this.fileName = fileName;
        this.flag = flag;
        this.fuel_level = fuel_level;
        this.odometer = odometer;
        this.heading = heading;
        this.latitude = latitude;
        this.speed = speed;
        this.read_datetime = read_datetime;
        this.capture_datetime = capture_datetime;
        this.warning = warning;
        this.tsp_provider = tsp_provider;
        this.id = id;
        this.longitude = longitude;
    }
    public UTLatest(String vin, String fileName, String flag, String fuel_level, String odometer, String heading,
                         double latitude, String speed, String read_datetime, String capture_datetime, String[] warning,
                         String tsp_provider, String id, double longitude) {
        super();
        this.vin = vin;
        this.fileName = fileName;
        this.flag = flag;
        this.fuel_level = fuel_level;
        this.odometer = odometer;
        this.heading = heading;
        this.latitude = latitude;
        this.speed = speed;
        this.read_datetime = read_datetime;
        this.capture_datetime = capture_datetime;
        this.warning = new ArrayList<String>();
        Arrays.asList(warning);
        this.tsp_provider = tsp_provider;
        this.id = id;
        this.longitude = longitude;
    }
    public String getVin() {
        return vin;
    }
    public void setVin(String vin) {
        this.vin = vin;
    }
    public String getFileName() {
        return fileName;
    }
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    public String getFlag() {
        return flag;
    }
    public void setFlag(String flag) {
        this.flag = flag;
    }
    public String getFuel_level() {
        return fuel_level;
    }
    public void setFuel_level(String fuel_level) {
        this.fuel_level = fuel_level;
    }
    public String getOdometer() {
        return odometer;
    }
    public void setOdometer(String odometer) {
        this.odometer = odometer;
    }
    public String getHeading() {
        return heading;
    }
    public void setHeading(String heading) {
        this.heading = heading;
    }
    public double getLatitude() {
        return latitude;
    }
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
    public String getSpeed() {
        return speed;
    }
    public void setSpeed(String speed) {
        this.speed = speed;
    }
    public String getRead_datetime() {
        return read_datetime;
    }
    public void setRead_datetime(String read_datetime) {
        this.read_datetime = read_datetime;
    }
    public String getCapture_datetime() {
        return capture_datetime;
    }
    public void setCapture_datetime(String capture_datetime) {
        this.capture_datetime = capture_datetime;
    }
    public List<String> getWarning() {
        return warning;
    }
    public void setWarning(List<String> warning) {
        this.warning = warning;
    }
    public String getTsp_provider() {
        return tsp_provider;
    }
    public void setTsp_provider(String tsp_provider) {
        this.tsp_provider = tsp_provider;
    }
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public double getLongitude() {
        return longitude;
    }
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    @Override
    public String toString() {
        return "UTLatest [vin=" + vin + ", fileName=" + fileName + ", flag=" + flag + ", fuel_level=" + fuel_level
                + ", odometer=" + odometer + ", heading=" + heading + ", latitude=" + latitude + ", speed=" + speed
                + ", read_datetime=" + read_datetime + ", capture_datetime=" + capture_datetime + ", warning=" + warning
                + ", tsp_provider=" + tsp_provider + ", id=" + id + ", longitude=" + longitude + "]";
    }

    //custom methods
    public Boolean isRepeatedEvent(UTLatest o){
        StringBuffer osb = new StringBuffer();
        osb.append(o.getHeading()).append("|").append(o.getSpeed()).append("|").append(o.getLatitude())
                .append("|").append(o.getLongitude());
        StringBuffer csb = new StringBuffer();
        csb.append(this.heading).append("|").append(this.speed).append("|").append(this.latitude)
                .append("|").append(this.longitude);
        if (1== osb.toString().compareTo(csb.toString()))
            return true;
        else
            return false;
    }
    //Strip time portion and return date only (yyyy-mm-dd)
    public String getReadDatePart() {
        if (null != this.read_datetime){
            return this.read_datetime.substring(0, this.read_datetime.indexOf("T"));
        }else return null;
    }

    public String getCaptureDatePart() {
        if (null != this.capture_datetime){
            return this.capture_datetime.substring(0, this.capture_datetime.indexOf("T"));
        }else return null;
    }
}
