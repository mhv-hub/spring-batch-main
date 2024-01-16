package com.mhv.batchprocessing.util;

import java.time.Duration;
import java.time.LocalDateTime;

public class Helper {

    public static String getDateTimeDifference(LocalDateTime start, LocalDateTime end){

        Duration duration = Duration.between(start, end);
        long days = duration.toDays();
        long hours = duration.toHoursPart();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        long milliseconds = duration.toMillisPart();

        if (days > 0) return days + " days " + hours + " hours " + minutes + " minutes " + seconds + " seconds " + milliseconds + " milliseconds";
        else if (hours > 0) return hours + " hours " + minutes + " minutes " + seconds + " seconds " + milliseconds + " milliseconds";
        else if (minutes > 0) return minutes + " minutes " + seconds + " seconds " + milliseconds + " milliseconds";
        else if (seconds > 0) return seconds + " seconds " + milliseconds + " milliseconds";
        else return milliseconds + " milliseconds";
    }
}
