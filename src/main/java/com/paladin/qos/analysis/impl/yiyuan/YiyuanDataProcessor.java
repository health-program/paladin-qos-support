package com.paladin.qos.analysis.impl.yiyuan;

import com.paladin.framework.core.exception.SystemException;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataProcessor;
import com.paladin.qos.model.data.DataUnit;

public abstract class YiyuanDataProcessor extends DataProcessor {

	protected String getDataSourceByUnit(String unitId) {
		DataUnit unit = DataConstantContainer.getUnit(unitId);
		if (unit != null) {
			String dbCode = unit.getDbCode();
			if (dbCode != null && dbCode.length() > 0) {
				return dbCode;
			}
		}

		throw new SystemException("找不到单位[" + unitId + "]对应的数据源");
	}

}
