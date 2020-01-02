package com.paladin.qos.analysis.impl.yiyuan.score.guanlianxing;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.yiyuan.YiyuanDataProcessor;
import com.paladin.qos.dynamic.mapper.yiyuan.score.guanlianxing.GuanlianxingMapper;
/**
 * 收费表
 * @author Administrator
 *
 */
@Component
public class Costrecord extends YiyuanDataProcessor{

	@Autowired
    private SqlSessionContainer sqlSessionContainer;
	
	public static final String EVENT_ID = "92004";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(GuanlianxingMapper.class).getCostrecordTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
    	sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(GuanlianxingMapper.class).getCostrecordEventNum(startTime,endTime,unitId);
    }
}
