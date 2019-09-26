package com.paladin.qos.service.analysis.data;

import com.paladin.qos.analysis.DataByUnit;

public class AnalysisUnit extends DataByUnit{
	
	private long eventNum;
	private long totalNum;
	
	public long getEventNum() {
		return eventNum;
	}

	public void setEventNum(long eventNum) {
		this.eventNum = eventNum;
	}

	public long getTotalNum() {
		return totalNum;
	}

	public void setTotalNum(long totalNum) {
		this.totalNum = totalNum;
	}

}
