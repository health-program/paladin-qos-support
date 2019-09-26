package com.paladin.qos.service.analysis.data;

import java.util.List;

import com.paladin.qos.analysis.DataByUnit;

public class DataPointUnit<T> extends DataByUnit{

	private List<T> points;

	public DataPointUnit(String unitId, List<T> points) {
		this.setUnitId(unitId);
		this.points = points;
	}


	public List<T> getPoints() {
		return points;
	}

	public void setPoints(List<T> points) {
		this.points = points;
	}

}
