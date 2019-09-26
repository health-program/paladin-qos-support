package com.paladin.common.util;

import java.util.Calendar;
import java.util.Date;

public class TimeIntervalUtil {

    public static Boolean canAdd(Date recordDate,int month){
            Calendar c = Calendar.getInstance();
            c.setTime(recordDate);
            c.add(Calendar.MONTH, month);
            Date currentTime = new Date();
            if (c.getTime().after(currentTime)) {
                return false;
            }
        return true;
    }
}
