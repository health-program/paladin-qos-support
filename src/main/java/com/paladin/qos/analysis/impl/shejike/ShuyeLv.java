package com.paladin.qos.analysis.impl.shejike;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.DataProcessor;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.dynamic.mapper.shejike.SheJiKeMapper;

public class ShuyeLv extends DataProcessor{
	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "16009";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(SheJiKeMapper.class).getTotalRecipe(startTime, endTime, unitId);
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(SheJiKeMapper.class).getEventShuyeLv(startTime, endTime, unitId);
	}
}
