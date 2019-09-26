package com.paladin.qos.model.countantibiotics;

import javax.persistence.Id;

import com.paladin.framework.common.BaseModel;

public class CountAntibiotics extends BaseModel {
    
    @Id
    private String id;
    private Integer varieties;
    private Double averageCost;
    private Double userRate;
    private Double rateOfTotal;
    private String unitId;

    public String getId() {
	return id;
    }

    public void setId(String id) {
	this.id = id;
    }

    public Integer getVarieties() {
	return varieties;
    }

    public void setVarieties(Integer varieties) {
	this.varieties = varieties;
    }

    public Double getAverageCost() {
	return averageCost;
    }

    public void setAverageCost(Double averageCost) {
	this.averageCost = averageCost;
    }

    public Double getUserRate() {
	return userRate;
    }

    public void setUserRate(Double userRate) {
	this.userRate = userRate;
    }

    public Double getRateOfTotal() {
	return rateOfTotal;
    }

    public void setRateOfTotal(Double rateOfTotal) {
	this.rateOfTotal = rateOfTotal;
    }

    public String getUnitId() {
        return unitId;
    }

    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }

}
