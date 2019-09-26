package com.paladin.qos.analysis;

public abstract class DataByUnit {
	
	private String unitId;

	public String getUnitId() {
		return unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}
	
	public String getUnitName() {
		return DataConstantContainer.getUnitName(unitId);
	}

	
}
