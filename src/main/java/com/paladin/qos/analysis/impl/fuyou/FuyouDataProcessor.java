package com.paladin.qos.analysis.impl.fuyou;

import java.util.Date;

import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataProcessor;
import com.paladin.qos.analysis.Metadata;
import com.paladin.qos.model.data.DataUnit;

public abstract class FuyouDataProcessor extends DataProcessor{

	private String getMappingUnitId(String unitId) {		
		DataUnit unit = DataConstantContainer.getUnit(unitId);
		return unit == null ? null : unit.getFuyouCode();
	}

	public Metadata processByDay(Date startTime, Date endTime, String unitId) {
		String mappingUnitId = getMappingUnitId(unitId);
		Metadata data = super.processByDay(startTime, endTime, mappingUnitId);
		if (data != null) {
			data.setUnitValue(unitId);
		}
		return data;
	}
	
}
