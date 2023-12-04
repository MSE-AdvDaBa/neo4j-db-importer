package mse.advdaba;

import java.util.Date;

public class DateTimeUtils {
    //1 minute = 60 seconds
    //1 hour = 60 x 60 = 3600
    //1 day = 3600 x 24 = 86400
    public String getDifference(Date startDate, Date endDate){

        //milliseconds
        long difference = endDate.getTime() - startDate.getTime();

        System.out.println("START : " + startDate);
        System.out.println("END : "+ endDate);

        long secondsInMilli = 1000;
        long minutesInMilli = secondsInMilli * 60;
        long hoursInMilli = minutesInMilli * 60;
        long daysInMilli = hoursInMilli * 24;

        long elapsedDays = difference / daysInMilli;
        difference = difference % daysInMilli;

        long elapsedHours = difference / hoursInMilli;
        difference = difference % hoursInMilli;

        long elapsedMinutes = difference / minutesInMilli;
        difference = difference % minutesInMilli;

        long elapsedSeconds = difference / secondsInMilli;

        return String.format(
                "%d days, %d hours, %d minutes, %d seconds",
                elapsedDays,
                elapsedHours, elapsedMinutes, elapsedSeconds);

    }

}