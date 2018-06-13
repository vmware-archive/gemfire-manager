/**
 * 
 */
package io.pivotal.pde.penske.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.pivotal.pde.penske.unittelemetry.aggregator.model.UnitTelemetry;

/**
 * @author spaladugu
 *
 */
@Component
public class LoadGenerator {
	private static final Log log = LogFactory.getLog(LoadGenerator.class.getName());
	@Autowired
	private DataLoaderProperties appProperties;

	private List<String> vins;

	private String[] direction = new String[]{"NE", "E", "SE", "E", "NW", "W"};
	
	private String[] latlongs = new String[] {
			"44.968046:-94.420307","44.33328:-89.132008", "33.755787:-116.359998",
			"33.844843:-116.54911", "44.92057:-93.44786", "44.240309:-91.493619",
			"44.968041:-94.419696", "44.333304:-89.132027", "33.755783:-116.360066",
			"33.844847:-116.549069", "44.920474:-93.447851", "44.240304:-91.493768"
	};

	private String[] dates = new String[] {"2018-06-02", "2018-06-03", "2018-06-04", "2018-06-05", "2018-06-06"};

	private double[] lats = new double[] {
			44.968046, 44.33328, 33.755787,
			33.844843, 44.92057, 44.240309,
			44.968041, 44.333304, 33.755783,
			33.844847, 44.920474, 44.240304
	};

	private double[] longs = new double[] {
			-94.420307, -89.132008, -116.359998,
			-116.54911, -93.44786, -91.493619,
			-94.419696, -89.132027, -116.360066,
			-116.549069, -93.447851, -91.493768
	};
	
	private String[] odometer = new String[]{"120000", "312312", "78612", "91278", "8097"};
	
	private String[] speed = new String[]{"10", "35", "55", "67", "90","5", "23"};
	
	private List<String> tsps;
	
	public Set<UnitTelemetry> generate() {
		int count = 0;
		Set<UnitTelemetry> unitTelemetrySet = new HashSet<UnitTelemetry>();
		if(null == vins || vins.size() == 0){
			loadVins();
		}
		if(null == tsps || tsps.size() == 0){
			loadTsps();
		}
		while (count < this.appProperties.getBatchsize()) {
			 unitTelemetrySet.add(generate_ut());
			count++;
		}
		return unitTelemetrySet;
	}

	public Set<UTLatest> generate1() {
		int count = 0;
		Set<UTLatest> unitTelemetryLatestSet = new HashSet<>();
		if(null == vins || vins.size() == 0){
			loadVins();
		}
		if(null == tsps || tsps.size() == 0){
			loadTsps();
		}
		while (count < this.appProperties.getBatchsize()) {
			unitTelemetryLatestSet.add(generate_utl());
			count++;
		}
		return unitTelemetryLatestSet;
	}

	private void loadVins() {
		File file = null;
		BufferedReader b = null;
		try {
			vins = new ArrayList<String>();
			file = new File(/*getClass().getClassLoader().getResource(*/"/tmp/vinnumbers.txt"/*).getFile()*/);
			b = new BufferedReader(new FileReader(file));
			String readLine = "";
			log.debug("Reading vinnumbers.txt ....");
			while ((readLine = b.readLine()) != null) {
				vins.add(readLine);
			}
			b.close();
		} catch (Exception e) {
			throw new RuntimeException("Error loading vinnumbers.txt file.", e);
		}finally{
			if(null != b){
				try {
					b.close();
				} catch (IOException e) {
					log.warn("error closing buffer.....");
				}
			}
		}
	}

	private void loadTsps() {
		File file = null;
		BufferedReader b = null;
		try {
			tsps = new ArrayList<String>();
			file = new File(/*getClass().getClassLoader().getResource(*/"/tmp/tsps.txt"/*).getFile()*/);
			b = new BufferedReader(new FileReader(file));
			String readLine = "";
			log.debug("Reading tsps.txt ....");
			while ((readLine = b.readLine()) != null) {
				tsps.add(readLine);
			}
			b.close();
			
		} catch (Exception e) {
			throw new RuntimeException("Error loading tsps.txt file.", e);
		}finally{
			if(null != b){
				try {
					b.close();
				} catch (IOException e) {
					log.warn("error closing buffer.....");
				}
			}
		}
	}
	 private UnitTelemetry generate_ut() {
	 SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	 String vin = (String) this.vins.get(new Random().nextInt(this.vins.size()));
	 String dt = dtFmt.format(Calendar.getInstance().getTime());
	 String direction = (String)this.direction[new Random().nextInt(this.direction.length)];
	 String latlong = (String)this.latlongs[new Random().nextInt(this.latlongs.length)];
	 String[] tokens = latlong.split(":");
	 String latitude = tokens[0];
	 String longitude = tokens[1];
	 String odometer = (String)this.odometer[new Random().nextInt(this.odometer.length)];
	 String speed = (String)this.speed[new Random().nextInt(this.speed.length)];
	 String tsp = (String) this.tsps.get(new Random().nextInt(this.tsps.size()));
	 UnitTelemetry ut = new UnitTelemetry();
	 ut.setId(vin+"|"+dt);
	 ut.setVin(vin);
	 //String date = dt.substring(0, dt.indexOf(" "));
	 String date = this.dates[new Random().nextInt(this.dates.length)];
	 String time = dt.substring(dt.indexOf(" "));
	 ut.setCapture_datetime(date + "T" + time);
	 ut.setFileName("Some filename");
	 ut.setFlag("someflag");
	 ut.setFuel_level("Some Fuel Level");
	 ut.setHeading(direction);
	 ut.setLatitude(latitude);
	 ut.setLongitude(longitude);
	 ut.setOdometer(odometer);
	 ut.setSpeed(speed);
	 ut.setRead_datetime(dt);
	 ut.setTsp_provider(tsp);
	 ut.setWarning(new ArrayList<String>());
	 return ut;
	 }

	private UTLatest generate_utl() {
		SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String vin = (String) this.vins.get(new Random().nextInt(this.vins.size()));
		String dt = dtFmt.format(Calendar.getInstance().getTime());
		String direction = (String)this.direction[new Random().nextInt(this.direction.length)];
		//String latlong = (String)this.latlongs[new Random().nextInt(this.latlongs.length)];
		double latitude = this.lats[new Random().nextInt(this.lats.length)];
		double longitude = this.longs[new Random().nextInt(this.longs.length)];
		String odometer = (String)this.odometer[new Random().nextInt(this.odometer.length)];
		String speed = (String)this.speed[new Random().nextInt(this.speed.length)];
		String tsp = (String) this.tsps.get(new Random().nextInt(this.tsps.size()));
		UTLatest utl = new UTLatest();
		utl.setId(vin+"|"+dt);
		utl.setVin(vin);
		String date = dt.substring(0, dt.indexOf(" "));
		String time = dt.substring(dt.indexOf(" "));
		utl.setCapture_datetime(date + "T" + time);
		utl.setFileName("Some filename");
		utl.setFlag("someflag");
		utl.setFuel_level("Some Fuel Level");
		utl.setHeading(direction);
		utl.setLatitude(latitude);
		utl.setLongitude(longitude);
		utl.setOdometer(odometer);
		utl.setSpeed(speed);
		utl.setRead_datetime(dt);
		utl.setTsp_provider(tsp);
		utl.setWarning(new ArrayList<String>());
		return utl;
	}
}
