package utilites;

import java.util.HashMap;
import java.util.Map;

public class AirportCorrespondence {
    private Map<String,String> map;
    private static AirportCorrespondence instance=null;

    private AirportCorrespondence(){
        map=new HashMap<String,String>();
    }

    public Map<String,String> getAirlineCorrespondence(){
        map.put("Banglore","BLR");
        map.put("Chennai","MAA");
        map.put("Delhi","DEL");
        map.put("Kolkata","CCU");
        map.put("Mumbai","BOM");
        map.put("Cochin","COK");
        map.put("Hyderabad","HYD");
        map.put("New Delhi","DEL");
        return map;
    }

    public static AirportCorrespondence getInstance(){
        if(instance==null){
            instance=new AirportCorrespondence();
        }
        return instance;
    }
}
