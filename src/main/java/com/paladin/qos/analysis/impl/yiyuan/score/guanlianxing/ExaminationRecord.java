package com.paladin.qos.analysis.impl.yiyuan.score.guanlianxing;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.yiyuan.YiyuanDataProcessor;
import com.paladin.qos.dynamic.mapper.yiyuan.score.guanlianxing.GuanlianxingMapper;
@Component
public class ExaminationRecord extends YiyuanDataProcessor{
	@Autowired
    private SqlSessionContainer sqlSessionContainer;
	
	public static final String EVENT_ID = "92002";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(GuanlianxingMapper.class).getExaminationRecordTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
    	sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(GuanlianxingMapper.class).getExaminationRecordEventNum(startTime,endTime,unitId);
    }
}
