package com.paladin.qos.model.report;

import javax.persistence.Id;

import com.paladin.framework.common.BaseModel;
import com.paladin.qos.analysis.DataConstantContainer;

/**   
 * @author MyKite
 * @version 2019年8月23日 下午4:05:12 
 */
public class ReportData extends BaseModel{

    public static final int REPORT_DATA_PERINATAL =1;
    public static final int REPORT_DATA_EPIDEMIC =2;
    public static final int REPORT_DATA_INFO =3;
    
    @Id
    private String id;
    private Integer type;
    private String data;
    private String unitId;
    
    public String getUnitName() {
	return DataConstantContainer.getUnitName(unitId);
    }
    
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public Integer getType() {
        return type;
    }
    public void setType(Integer type) {
        this.type = type;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
    public String getUnitId() {
        return unitId;
    }
    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }
    
}
