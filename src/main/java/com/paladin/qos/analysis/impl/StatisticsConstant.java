package com.paladin.qos.analysis.impl;

import java.text.DateFormat;
import java.text.ParseException;

import com.paladin.framework.utils.time.DateFormatUtil;

/**   
 * @author MyKite
 * @version 2019年11月27日 下午7:22:32 
 */
public class StatisticsConstant {

    public final static long TABLE_MEDICALRECORD_BF_START_TIME;
    
    static {
	try {
	    DateFormat format = DateFormatUtil.getThreadSafeFormat("yyyy-MM-dd");
	    TABLE_MEDICALRECORD_BF_START_TIME = format.parse("2019-11-01").getTime();
	} catch (ParseException e) {
	    throw new RuntimeException();
	}
    }
    
}
