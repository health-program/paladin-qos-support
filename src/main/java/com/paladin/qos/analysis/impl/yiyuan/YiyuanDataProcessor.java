package com.paladin.qos.analysis.impl.yiyuan;

import com.paladin.framework.core.exception.SystemException;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataConstantContainer.Unit;
import com.paladin.qos.analysis.DataProcessor;

public abstract class YiyuanDataProcessor extends DataProcessor {

	protected String getDataSourceByUnit(String unitId) {
		Unit unit = DataConstantContainer.getUnit(unitId);
		if (unit != null) {
			String dbCode = unit.getSource().getDbCode();
			if (dbCode != null && dbCode.length() > 0) {
				return dbCode;
			}
		}

		throw new SystemException("找不到单位[" + unitId + "]对应的数据源");
	}

}
