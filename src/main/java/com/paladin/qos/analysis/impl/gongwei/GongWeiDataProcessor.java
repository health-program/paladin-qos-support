package com.paladin.qos.analysis.impl.gongwei;

import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataProcessUnit;
import com.paladin.qos.analysis.DataProcessor;

/**
 * @author MyKite
 * @version 2019年9月11日 上午9:14:03
 */
public abstract class GongWeiDataProcessor extends DataProcessor {

	public String getMappingUnitId(String unitId) {
		DataProcessUnit unit = DataConstantContainer.getUnit(unitId);
		return unit == null ? null : unit.getSource().getGongweiCode();
	}
}
