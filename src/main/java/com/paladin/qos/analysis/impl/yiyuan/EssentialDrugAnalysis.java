package com.paladin.qos.analysis.impl.yiyuan;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.yiyuan.EssentialDrugAnalysisMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * <基本药物分析>
 *
 * @author Huangguochen
 * @create 2019/11/18 17:13
 */
@Component
public class EssentialDrugAnalysis extends YiyuanDataProcessor{
    @Autowired
    private SqlSessionContainer sqlSessionContainer;

    public static final String EVENT_ID = "12103";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(EssentialDrugAnalysisMapper.class).getTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(EssentialDrugAnalysisMapper.class).getEventNum(startTime,endTime,unitId);
    }
}
