package com.paladin.qos.analysis.impl.yiyuan;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.yiyuan.HospitalPerformanceAppraisalMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * <Ⅰ类切口手术-感染数>
 *
 * @author Huangguochen
 * @create 2019/11/28 13:19
 */
@Component
public class OperationsInfection extends YiyuanDataProcessor{
    @Autowired
    private SqlSessionContainer sqlSessionContainer;

    public static final String EVENT_ID = "70006";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(HospitalPerformanceAppraisalMapper.class).getOperationsInfectionTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
        return 0;
    }
}
