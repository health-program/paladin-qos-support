package com.paladin.qos.analysis.impl.yiyuan.opd;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.StatisticsConstant;
import com.paladin.qos.analysis.impl.yiyuan.YiyuanDataProcessor;
import com.paladin.qos.dynamic.mapper.yiyuan.opd.OpdStatisticsMapper;

/**门诊人次数   
 * @author MyKite
 * @version 2019年9月27日 上午9:43:09 
 */
@Component
public class OutpatientNum extends YiyuanDataProcessor{

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "31008";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
		if(unitId.equals("320583467170249")||unitId.equals("320583467170257")||unitId.equals("320583467170513")||unitId.equals("320583467170265")){
		    if(startTime.getTime()<= StatisticsConstant.TABLE_MEDICALRECORD_BF_START_TIME){
			return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).outpatientNum(startTime, endTime);
		    }else{
			return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).outpatientNumFour(startTime, endTime);
		    }	
		}else{
			return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).outpatientNum(startTime, endTime);
		}
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		return 0;
	}
}
