package com.paladin.qos.analysis.impl.yiyuan.opd;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.StatisticsConstant;
import com.paladin.qos.analysis.impl.yiyuan.YiyuanDataProcessor;
import com.paladin.qos.dynamic.mapper.yiyuan.opd.OpdStatisticsMapper;

/**
 * 门诊总人次
 * 
 * @author MyKite
 * @version 2019年9月17日 上午9:56:41
 */
@Component
public class OPDTotal extends YiyuanDataProcessor {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "31001";
	
	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
		if(unitId.equals("320583467170249")||unitId.equals("320583467170257")||unitId.equals("320583467170513")||unitId.equals("320583467170265")){
			if(startTime.getTime()<= StatisticsConstant.TABLE_MEDICALRECORD_BF_START_TIME){
			    return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).OPDTotal(startTime, endTime, unitId); 
			}else{
			    return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).OPDTotalFour(startTime, endTime, unitId);
			}
		}else{
		    return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).OPDTotal(startTime, endTime, unitId);
		}
		
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		return 0;
	}
}
