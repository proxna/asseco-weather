import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) {
        SqlManager sqlManager= null;
        try {
            System.out.println("Init database driver");
            sqlManager = new SqlManager();
        } catch (ClassNotFoundException e) {
            System.out.println("Class Error. Can't find sql class. Program will be closed");
            System.exit(1);
        } catch (SQLException e) {
            System.out.println("Db Error. Can't connect to db. Program will be closed");
            e.printStackTrace();
            System.exit(1);
        }

        Calendar calendar=Calendar.getInstance();
        calendar.add(Calendar.DATE, -3);
        DateFormat df=new SimpleDateFormat("yyyy-MM-dd");
        DateFormat timeformat=new SimpleDateFormat("HH:mm");
        DateFormat hourformat=new SimpleDateFormat("HH");

        Scanner scan=new Scanner(System.in);

        String addQuery="add [A-Za-z]+";
        String deleteQuery="delete [A-Za-z]+";
        String getallQuery="get";
        String getHourQuery="get [0-9]{2}";
        String getCityQuery="get [A-Za-z]+";
        String getCityandHourQuery="get [A-Za-z]+ [0-9]{2}";
        String citylistQuery="citylist";
        String helpQuery="help";
        String exitQuery="exit";

        String hourregex="[0-9]{2}:00";

        try {
            System.out.println("Deleted old data");
            sqlManager.deleteByDate(df.format(calendar.getTime()));
        } catch (SQLException e) {
            System.out.println("Db Error. Can't connect to db. Program will be closed");
            System.exit(1);
        }

        System.out.println("Hello. It's me. Your local weather.");

        do{
            if(Pattern.matches(hourregex, timeformat.format(calendar.getTime()))){
                try {
                    sqlManager.insert(sqlManager.addRecords(Integer.parseInt(hourformat.format(calendar.getTime()))));
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    System.exit(1);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(1);
                } catch (IOException e) {
                    System.out.println("Internet Connection Error. Can't connect to web. Program will be closed");
                    System.exit(1);
                }
            };
            String query=scan.nextLine();
            if(Pattern.matches(addQuery, query)){
                String[] tmp=query.split(" ");
                try {
                    sqlManager.insertCity(tmp[1]);
                    System.out.println("New city added");
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    System.exit(1);
                } catch (IOException e) {
                    System.out.println("Internet Connection Error. Can't connect to web. Program will be closed");
                    System.exit(1);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            else if(Pattern.matches(deleteQuery, query)){
                String[] tmp=query.split(" ");
                try {
                    if(sqlManager.selectCity(tmp[1])!=null){
                        sqlManager.delete(tmp[1]);
                        System.out.println("City deleted");
                    }
                    else{
                        System.out.println("This city isn't in list.");
                    }
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    break;
                }
            }
            else if(Pattern.matches(getallQuery, query)){
                List<WeatherRecord> data= null;
                try {
                    data = sqlManager.select();
                    printResult(data);
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    System.exit(1);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(1);
                } catch (IOException e) {
                    System.out.println("Internet Connection Error. Can't connect to web. Program will be closed");
                    System.exit(1);
                }
            }
            else if(Pattern.matches(getCityQuery, query)){
                String[] tmp=query.split(" ");
                try {
                    if(sqlManager.selectCity(tmp[1])!=null){
                        List<WeatherRecord> data = sqlManager.select(tmp[1]);
                        printResult(data);
                    }
                    else{
                        System.out.println("This city isn't on the list.");
                    }
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    e.printStackTrace();
                    System.exit(1);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(1);
                } catch (IOException e) {
                    System.out.println("Internet Connection Error. Can't connect to web. Program will be closed");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            else if(Pattern.matches(getHourQuery, query)){
                String[] tmp=query.split(" ");
                try {
                    List<WeatherRecord> data = sqlManager.select(Integer.parseInt(tmp[1]));
                    printResult(data);
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    e.printStackTrace();
                    System.exit(1);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(1);
                } catch (IOException e) {
                    System.out.println("Internet Connection Error. Can't connect to web. Program will be closed");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            else if(Pattern.matches(getCityandHourQuery, query)){
                String[] tmp=query.split(" ");
                try {
                    if(sqlManager.selectCity(tmp[1])!=null){
                        List<WeatherRecord> data = sqlManager.select(tmp[1], Integer.parseInt(tmp[2]));
                        printResult(data);
                    }
                    else{
                        System.out.println("This city isn't on the list.");
                    }
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    e.printStackTrace();
                    System.exit(1);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    System.exit(1);
                } catch (IOException e) {
                    System.out.println("Internet Connection Error. Can't connect to web. Program will be closed");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            else if(Pattern.matches(exitQuery, query)){
                System.out.println("Thanks for use me :)");
                System.exit(0);
            }
            else if(Pattern.matches(citylistQuery, query)){
                try {
                    List<City> data=sqlManager.selectAllCities();
                    if(data.isEmpty()){
                        System.out.println("Cities list is empty. Please add some city(command 'add').");
                    }
                    else{
                        int i=0;
                        for(City obj : data){
                            System.out.println((i+1) + " " + obj.name);
                            ++i;
                        }
                    }
                } catch (SQLException e) {
                    System.out.println("Db Error. Can't connect to db. Program will be closed");
                    break;
                }
            }
            else if(Pattern.matches(helpQuery, query)){
                System.out.println("add [city name] - add city to list");
                System.out.println("delete [city name] - remove city from list");
                System.out.println("citylist - display all cities from list");
                System.out.println("get - display current weather for all cities from the list");
                System.out.println("get [city] - display current weather for selected city from the list");
                System.out.println("get [hour in HH format] - display weather for selected hour and all cities from the list");
                System.out.println("get [city name] [hour in HH format] - display weather for selected hour and selected city");
            }
            else{
                System.out.println("Unknown command");
            }
        }while(true);
        //System.out.println("Hello World!");
    }

    private static void printResult(List<WeatherRecord> results){
        System.out.println("City\tTemperature\tWeather\tWindspeed\tDate\tHour");
        for(WeatherRecord obj : results){
            System.out.print(obj.city+"\t");
            System.out.print(obj.temperature+"\t");
            System.out.print(obj.weather+"\t");
            System.out.print(obj.windspeed+"\t");
            System.out.print(obj.date+"\t");
            System.out.print(obj.hour+"\n");
        }
    }
}
