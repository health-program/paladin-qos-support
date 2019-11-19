package com.paladin.qos.analysis.impl.yiyuan;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.dynamic.mapper.yiyuan.PaymentAnalysisMapper;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

/**
 * <商业保险>
 *
 * @author Huangguochen
 * @create 2019/11/19 10:48
 */
public class BusinessInsurancePayment extends YiyuanDataProcessor{
    @Autowired
    private SqlSessionContainer sqlSessionContainer;

    public static final String EVENT_ID = "42008";

    @Override
    public String getEventId() {
        return EVENT_ID;
    }

    @Override
    public long getTotalNum(Date startTime, Date endTime, String unitId) {
        sqlSessionContainer.setCurrentDataSource(getDataSourceByUnit(unitId));
        return sqlSessionContainer.getSqlSessionTemplate().getMapper(PaymentAnalysisMapper.class).getBusinessInsuranceTotalNum(startTime,endTime,unitId);
    }

    @Override
    public long getEventNum(Date startTime, Date endTime, String unitId) {
        return 0;
    }
}
