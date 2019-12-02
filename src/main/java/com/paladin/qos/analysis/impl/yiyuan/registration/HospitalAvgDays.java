package com.paladin.qos.analysis.impl.yiyuan.registration;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.yiyuan.YiyuanDataProcessor;
import com.paladin.qos.dynamic.mapper.yiyuan.opd.OpdStatisticsMapper;

/**医院平均住院日
 * @author MyKite
 * @version 2019年9月27日 上午9:43:09 
 */
@Component
public class HospitalAvgDays extends YiyuanDataProcessor{

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "41005";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	//天数
	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).getTotaldays(startTime, endTime);
	}
    //人次
	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(OpdStatisticsMapper.class).getTotalPeople(startTime, endTime);
	}
}
