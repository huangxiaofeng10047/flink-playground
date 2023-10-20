package org.sample.flink;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

public class OrderAgg {
    public String userId;
    public Double avgPriceAmount;
    public String userName;
    public Long eventTime;

    public static void main(String[] args) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            Date date = sdf.parse("2023-07-16T04:31:09Z");
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
            TemporalAccessor it=dateTimeFormatter.parse("2023-07-16T04:31:09Z");
            System.out.println(date);
            System.out.println(it);
        } catch (ParseException e) {
        }
    }
}
