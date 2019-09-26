package com.paladin.qos.service.analysis.data;

import java.util.List;

import com.paladin.qos.analysis.DataConstantContainer;

public class ValidateEventResult {

	private String eventId;
	private List<ValidateUnitResult> unitResults;
	
	public String getEventName() {
		return DataConstantContainer.getEventName(eventId);
	}
	
	public String getEventId() {
		return eventId;
	}
	
	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	
	public List<ValidateUnitResult> getUnitResults() {
		return unitResults;
	}
	
	public void setUnitResults(List<ValidateUnitResult> unitResults) {
		this.unitResults = unitResults;
	}

}
