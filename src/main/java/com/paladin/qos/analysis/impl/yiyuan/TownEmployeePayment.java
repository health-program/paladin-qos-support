package com.paladin.qos.analysis.impl.yiyuan;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.yiyuan.PaymentAnalysisMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * <城镇职工付费>
 *
 * @author Huangguochen
 * @create 2019/11/19 10:27
 */
@Component
public class TownEmployeePayment extends YiyuanDataProcessor{

    @Autowired
    private SqlSessionContainer sqlSessionContainer;

    public static final String EVENT_ID = "42005";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(PaymentAnalysisMapper.class).getTownEmployeeTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
        return 0;
    }

}
