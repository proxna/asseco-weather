import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by GPDellKonto on 2016-05-13.
 */
public class SqlManager {

    private Connection conn=null;
    private Statement stat;
    private Calendar calendar=Calendar.getInstance();
    private DateFormat df=new SimpleDateFormat("yyyy-MM-dd");
    private DateFormat hourformat=new SimpleDateFormat("HH");

    private String googleMapsUrl="https://maps.googleapis.com/maps/api/geocode/json?address=";
    private String forecastUrl="https://api.forecast.io/forecast/8ca372edaecb00828e1a5dd87dd9d0e7/";
    private String openWeatherUrl="http://api.openweathermap.org/data/2.5/weather?";
    private String googleApiKey="&key=AIzaSyCM2wxP_ZK3L5s7-47ZvME2s-w7k_MerHs";
    private String openWeatherKey="715cda8babf5bfcac1b9b09296efb4b4";

    public SqlManager() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        createTables();

    }

    private void open() throws SQLException {
        conn = DriverManager.getConnection("jdbc:sqlite:weather.db");
        stat = conn.createStatement();
    }

    private void close() throws SQLException {
        stat.close();
        conn.close();
    }

    private void createTables() throws SQLException {
        open();
        String queryCity="CREATE TABLE IF NOT EXISTS city ("
                + "id INTEGER PRIMARY KEY,"
                + "nameCity VARCHAR(255),"
                + "longtitude NUMERIC(3, 7),"
                + "latitude NUMERIC(3, 7)"
                + ");";
        String queryWeather="CREATE TABLE IF NOT EXISTS weather ("
                + "id INTEGER PRIMARY KEY,"
                + "city VARCHAR(255),"
                + "temperature NUMERIC(3, 1),"
                + "weather VARCHAR(255),"
                + "windspeed NUMERIC(3, 1),"
                + "dateStr VARCHAR(255),"
                + "hourStr INT"
                + ");";
        stat.executeUpdate(queryCity);
        stat.executeUpdate(queryWeather);
        close();
    }

    public List<WeatherRecord> select() throws SQLException, IOException, URISyntaxException {
        List<WeatherRecord> result=new ArrayList<WeatherRecord>();
        String query="SELECT * FROM weather WHERE hourStr="+Integer.parseInt(hourformat.format(calendar.getTime()))+";";
        open();
        ResultSet r=stat.executeQuery(query);
        WeatherRecord tmp;
        int records=0;
        while (r.next()){
            tmp=new WeatherRecord();
            tmp.city=r.getString("city");
            tmp.temperature=r.getDouble("temperature");
            tmp.weather=r.getString("weather");
            tmp.windspeed=r.getDouble("windspeed");
            tmp.date=r.getString("dateStr");
            tmp.hour=r.getInt("hourStr");
            result.add(tmp);
            ++records;
        }
        close();
        if(records==0){
            insert(addRecords(Integer.parseInt(hourformat.format(calendar.getTime()))));
            return select();
        }
        else{
            return result;
        }
    }

    public List<WeatherRecord> select(String city) throws SQLException, IOException, URISyntaxException {
        List<WeatherRecord> result=new ArrayList<WeatherRecord>();
        String query="SELECT * FROM weather WHERE city='"+city+"';";
        open();
        ResultSet r=stat.executeQuery(query);
        WeatherRecord tmp;
        int records = 0;
        while (r.next()){
            tmp=new WeatherRecord();
            tmp.city=r.getString("city");
            tmp.temperature=r.getDouble("temperature");
            tmp.weather=r.getString("weather");
            tmp.windspeed=r.getDouble("windspeed");
            tmp.date=r.getString("dateStr");
            tmp.hour=r.getInt("hourStr");
            result.add(tmp);
            ++records;
        }
        close();
        if(records==0){
            insert(addRecords(city));
            return select(city);
        }
        else{
            return result;
        }
    }

    public List<WeatherRecord> select(int time) throws SQLException, IOException, URISyntaxException {
        List<WeatherRecord> result=new ArrayList<WeatherRecord>();
        String query="SELECT * FROM weather WHERE hourStr="+time+";";
        open();
        ResultSet r=stat.executeQuery(query);
        WeatherRecord tmp;
        int records=0;
        while (r.next()){
            tmp=new WeatherRecord();
            tmp.city=r.getString("city");
            tmp.temperature=r.getDouble("temperature");
            tmp.weather=r.getString("weather");
            tmp.windspeed=r.getDouble("windspeed");
            tmp.date=r.getString("dateStr");
            tmp.hour=r.getInt("hourStr");
            result.add(tmp);
            ++records;
        }
        close();
        if(records==0){
            insert(addRecords(time));
            return select(time);
        }
        else{
            return result;
        }
    }

    public List<WeatherRecord> select(String city, int time) throws SQLException, IOException, URISyntaxException {
        List<WeatherRecord> result=new ArrayList<WeatherRecord>();
        String query="SELECT * FROM weather WHERE hourStr="+time+" AND city='"+city+"';";
        open();
        ResultSet r=stat.executeQuery(query);
        WeatherRecord tmp;
        int records=0;
        while (r.next()){
            tmp=new WeatherRecord();
            tmp.city=r.getString("city");
            tmp.temperature=r.getDouble("temperature");
            tmp.weather=r.getString("weather");
            tmp.windspeed=r.getDouble("windspeed");
            tmp.date=r.getString("dateStr");
            tmp.hour=r.getInt("hourStr");
            result.add(tmp);
            ++records;
        }
        close();
        if(records==0){
            insert(addRecords(city));
            insert(addRecords(time));
            return select(city, time);
        }
        else{
            return result;
        }
    }

    public List<City> selectAllCities() throws SQLException {
        ArrayList<City> result=new ArrayList<City>();
        String query="SELECT * FROM city;";
        open();
        ResultSet r=stat.executeQuery(query);
        while (r.next()){
            City tmp=new City(r.getString("nameCity"), r.getDouble("longtitude"), r.getDouble("latitude"));
            result.add(tmp);
        }
        close();
        return result;
    }

    public City selectCity(String city) throws SQLException {
        City result;
        open();
        String query="SELECT * FROM city WHERE nameCity='"+city+"';";
        ResultSet r=stat.executeQuery(query);
        if(r.next()){
            result=new City(r.getString("nameCity"), r.getDouble("longtitude"), r.getDouble("latitude"));
        }
        else{
            result=null;
        }
        close();
        return result;
    }

    public void insert(List<WeatherRecord> data) throws SQLException {
        open();
        for(WeatherRecord obj : data){
            String query="INSERT INTO weather VALUES(NULL,'"+obj.city+"',"+obj.temperature+",'"+obj.weather+"',"+obj.windspeed+",'"+obj.date+"',"+obj.hour+");";
            stat.execute(query);
        }
        close();
    }

    public void insertCity(String city) throws SQLException, IOException, URISyntaxException {
        City tmp=getCoordinates(city);
        open();
        String query="INSERT INTO city VALUES(NULL,'"+ tmp.name +"', "+ tmp.longtitude +", "+ tmp.latitude +");";
        stat.execute(query);
        close();
    }

    public void deleteByDate(String date) throws SQLException {
        open();
        String query="DELETE FROM weather WHERE dateStr='"+date+"';";
        stat.execute(query);
        close();
    }

    public void delete(String city) throws SQLException {
        open();
        String query="DELETE FROM city WHERE nameCity='"+city+"';";
        stat.execute(query);
        query="DELETE FROM weather WHERE city='"+city+"';";
        stat.execute(query);
        close();
    }
    private City getCoordinates(String city) throws IOException, URISyntaxException {
        double longtitude,latitude;
        URI queryUrl=new URI(googleMapsUrl+city+googleApiKey);
        JSONTokener tokener=new JSONTokener(queryUrl.toURL().openStream());
        JSONObject json=new JSONObject(tokener);
        JSONArray tmp=json.getJSONArray("results");
        json=tmp.getJSONObject(0).getJSONObject("geometry");
        json=json.getJSONObject("location");
        latitude=json.getDouble("lat");
        longtitude=json.getDouble("lng");
        City result=new City(city, longtitude, latitude);
        return result;
    }

    private List<WeatherRecord> addRecords(String city) throws SQLException, URISyntaxException, IOException {
        ArrayList<WeatherRecord> result=new ArrayList<WeatherRecord>();
        City tmp=selectCity(city);
        URI queryForecastUrl=new URI(forecastUrl+tmp.latitude+","+tmp.longtitude);
        URI queryOpenUrl=new URI(openWeatherUrl+"lat="+tmp.latitude+"&lon="+tmp.longtitude+"&appid="+openWeatherKey);
        JSONTokener forecastTokener=new JSONTokener(queryForecastUrl.toURL().openStream());
        JSONObject jsonForecast=new JSONObject(forecastTokener);
        JSONObject jsonWeather=jsonForecast.getJSONObject("currently");
        JSONTokener openTokener=new JSONTokener(queryOpenUrl.toURL().openStream());
        JSONObject jsonOpen=new JSONObject(openTokener);
        WeatherRecord weather=new WeatherRecord();
        weather.city=city;
        weather.temperature=fromFartoCel(jsonWeather.getDouble("temperature"));
        weather.weather=jsonWeather.getString("summary");
        weather.windspeed=(jsonWeather.getDouble("windSpeed")+jsonOpen.getJSONObject("wind").getDouble("speed"))/2;
        weather.date=df.format(calendar.getTime());
        weather.hour=Integer.parseInt(hourformat.format(calendar.getTime()));
        result.add(weather);
        return result;
    }
    public List<WeatherRecord> addRecords(int hour) throws SQLException, URISyntaxException, IOException {
        ArrayList<WeatherRecord> result=new ArrayList<WeatherRecord>();
        List<City> cities=selectAllCities();
        for(City obj : cities){
            URI queryForecastUrl=new URI(forecastUrl+obj.latitude+","+obj.longtitude);
            JSONTokener forecastTokener=new JSONTokener(queryForecastUrl.toURL().openStream());
            JSONObject jsonForecast=new JSONObject(forecastTokener);
            JSONObject jsonWeather=jsonForecast.getJSONObject("currently");
            WeatherRecord weather=new WeatherRecord();
            weather.city=obj.name;
            weather.temperature=fromFartoCel(jsonWeather.getDouble("temperature"));
            weather.weather=jsonWeather.getString("summary");
            weather.windspeed=jsonWeather.getDouble("windSpeed");
            weather.date=df.format(calendar.getTime());
            weather.hour=hour;
            result.add(weather);
        }
        return result;
    }

    private double fromFartoCel(double temperature){
        return ((temperature-32)*5)/9;
    }
}
