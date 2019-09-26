package com.paladin.qos.model.infectioncomplication;

import javax.persistence.Id;

import com.paladin.framework.common.BaseModel;

public class OperationComplication extends BaseModel {
   
    @Id
    private String id;
    private Double embolism;
    private Double pulmonaryEmbolism;
    private Double venousThrombosis;
    private Double hematoma;
    private Double woundDehiscence;
    private Double suddenDeath;
    private Double operativeComplications;
    private Double anaesthesia;
    private String unitId;
    
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public Double getEmbolism() {
        return embolism;
    }
    public void setEmbolism(Double embolism) {
        this.embolism = embolism;
    }
    public Double getPulmonaryEmbolism() {
        return pulmonaryEmbolism;
    }
    public void setPulmonaryEmbolism(Double pulmonaryEmbolism) {
        this.pulmonaryEmbolism = pulmonaryEmbolism;
    }
    public Double getVenousThrombosis() {
        return venousThrombosis;
    }
    public void setVenousThrombosis(Double venousThrombosis) {
        this.venousThrombosis = venousThrombosis;
    }
    public Double getHematoma() {
        return hematoma;
    }
    public void setHematoma(Double hematoma) {
        this.hematoma = hematoma;
    }
    public Double getWoundDehiscence() {
        return woundDehiscence;
    }
    public void setWoundDehiscence(Double woundDehiscence) {
        this.woundDehiscence = woundDehiscence;
    }
    public Double getSuddenDeath() {
        return suddenDeath;
    }
    public void setSuddenDeath(Double suddenDeath) {
        this.suddenDeath = suddenDeath;
    }
    public Double getOperativeComplications() {
        return operativeComplications;
    }
    public void setOperativeComplications(Double operativeComplications) {
        this.operativeComplications = operativeComplications;
    }
    public Double getAnaesthesia() {
        return anaesthesia;
    }
    public void setAnaesthesia(Double anaesthesia) {
        this.anaesthesia = anaesthesia;
    }
    public String getUnitId() {
        return unitId;
    }
    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }
    
}
