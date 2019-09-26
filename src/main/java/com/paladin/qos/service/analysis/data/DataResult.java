package com.paladin.qos.service.analysis.data;

import java.util.ArrayList;
import java.util.List;

import com.paladin.qos.analysis.DataConstantContainer;

public class DataResult<T> {

	private String eventId;
	private int dataType;

	private List<DataPointUnit<T>> unitPoints;

	public DataResult(String eventId, int dataType, List<DataPointUnit<T>> unitPoints) {
		this.eventId = eventId;
		this.dataType = dataType;
		this.unitPoints = unitPoints;
	}

	public DataResult(String eventId, int dataType, DataPointUnit<T> unitPoint) {
		this.eventId = eventId;
		this.dataType = dataType;
		this.unitPoints = new ArrayList<>();
		this.unitPoints.add(unitPoint);
	}
	
	public String getEventName() {
		return DataConstantContainer.getEventName(eventId);
	}	

	public String getEventId() {
		return eventId;
	}

	public void setEventId(String eventId) {
		this.eventId = eventId;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public List<DataPointUnit<T>> getUnitPoints() {
		return unitPoints;
	}

	public void setUnitPoints(List<DataPointUnit<T>> unitPoints) {
		this.unitPoints = unitPoints;
	}


}
