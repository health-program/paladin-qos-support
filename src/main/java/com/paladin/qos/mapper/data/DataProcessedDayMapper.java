package com.paladin.qos.mapper.data;

import com.paladin.qos.model.data.DataProcessedDay;
import com.paladin.framework.core.configuration.mybatis.CustomMapper;

public interface DataProcessedDayMapper extends CustomMapper<DataProcessedDay>{

	public int updateData(DataProcessedDay model);
	
}