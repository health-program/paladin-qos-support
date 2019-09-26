package com.paladin.qos.analysis.impl.gongwei.pressureSugar;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.thymeleaf.util.StringUtils;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.gongwei.GongWeiDataProcessor;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.dynamic.mapper.gongwei.PublicHealthManagementMapper;

/**
 * 高血压患者规范管理率
 * 
 * @author wcw
 *
 */
@Component
public class SugarManageRate extends GongWeiDataProcessor {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "22007";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		String gongWeiUnitId = getMappingUnitId(unitId);
		if (StringUtils.isEmpty(gongWeiUnitId)) {
			return 0;
		}
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(PublicHealthManagementMapper.class).getSugarFollowNumber(getStringYear(startTime, endTime),
				gongWeiUnitId);
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		String gongWeiUnitId = getMappingUnitId(unitId);
		if (StringUtils.isEmpty(gongWeiUnitId)) {
			return 0;
		}
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		return sqlSessionContainer.getSqlSessionTemplate().getMapper(PublicHealthManagementMapper.class).getSugarManageNumber(getStringYear(startTime, endTime),
				gongWeiUnitId);
	}

	private List<String> getStringYear(Date startTime, Date endTime) {
		List<String> yearStr = new ArrayList<>();
		Calendar start = Calendar.getInstance();
		start.setTime(startTime);
		int startYear = start.get(Calendar.YEAR);
		Calendar end = Calendar.getInstance();
		end.setTime(endTime);
		int endYear = end.get(Calendar.YEAR);
		if (endYear >= startYear) {
			for (int i = 0; i <= endYear - startYear; i++) {
				yearStr.add(String.valueOf(startYear + i));
			}
			return yearStr;
		}
		return null;
	}

}
