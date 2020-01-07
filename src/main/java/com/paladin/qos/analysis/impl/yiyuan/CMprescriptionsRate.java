package com.paladin.qos.analysis.impl.yiyuan;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.yiyuan.HospitalPerformanceAppraisalMapper;
/**
 * 中医科室中医处方占比
 * @author Administrator
 *
 */
@Component
public class CMprescriptionsRate extends YiyuanDataProcessor{

	@Autowired
    private SqlSessionContainer sqlSessionContainer;

    public static final String EVENT_ID = "70008";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(HospitalPerformanceAppraisalMapper.class).getCMprescriptionsTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
    	sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(HospitalPerformanceAppraisalMapper.class).getCMprescriptionsEventNum(startTime,endTime,unitId);
    }
}
