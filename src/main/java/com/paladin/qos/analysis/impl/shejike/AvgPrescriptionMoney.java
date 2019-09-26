package com.paladin.qos.analysis.impl.shejike;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.DataProcessor;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.dynamic.mapper.shejike.SheJiKeMapper;

/**
 * 平均处方金额
 * 
 * @author FM
 *
 */
public class AvgPrescriptionMoney extends DataProcessor {
	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "16005";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	// 处方数量
	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(SheJiKeMapper.class).getPrescriptionNumber(startTime, endTime, unitId);
	}

	// 处方金额
	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
		return (long) (sqlSessionContainer.getSqlSessionTemplate().getMapper(SheJiKeMapper.class).getPrescriptionMoney(startTime, endTime, unitId) * 100);
	}

}
