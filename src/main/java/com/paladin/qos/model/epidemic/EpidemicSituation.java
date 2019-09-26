package com.paladin.qos.model.epidemic;

import java.util.Date;
import javax.persistence.Id;
import com.paladin.framework.common.BaseModel;

public class EpidemicSituation extends BaseModel{
    
    @Id
    private String id;

    private String incidentUnit;//事发单位名称

    private String reportUnit;//报告单位（医疗机构为1，事发学校为2，家长为3，其他为4）

    private String sicknessClassify;//病种分类

    private String grade;//班级

    private String region;//区域

    private Integer firstWeek;//发病人数（首周）

    private Integer total;//发病人数（累计）

    private Integer vaccinationGradeNumber;//应急接种班级数

    private Integer vaccinationPeopleNumber;//应急接种人数

    private Integer suspendGradeNumber;//停课班级数

    private Integer suspendPeopleNumber;//停课人数

    private Date suspendData;//停课时间

    private Integer municipalLeveNumber;//出动人次数(市级专业部门)

    private Integer districtTownNumber;//出动人次数(区镇级专业部门)

    private Integer isEarlyWarningValue;//是否预警值(是为1，否为2)

    private Integer isReasonTraceability;//是否原因溯源(是为1，否为2)

    private Integer isTraceabilityClear;//溯源是否清楚(是为1，否为2)

    private String remarks;//备注

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id == null ? null : id.trim();
    }

    public String getIncidentUnit() {
        return incidentUnit;
    }

    public void setIncidentUnit(String incidentUnit) {
        this.incidentUnit = incidentUnit == null ? null : incidentUnit.trim();
    }

    public String getReportUnit() {
        return reportUnit;
    }

    public void setReportUnit(String reportUnit) {
        this.reportUnit = reportUnit == null ? null : reportUnit.trim();
    }

    public String getSicknessClassify() {
        return sicknessClassify;
    }

    public void setSicknessClassify(String sicknessClassify) {
        this.sicknessClassify = sicknessClassify == null ? null : sicknessClassify.trim();
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade == null ? null : grade.trim();
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region == null ? null : region.trim();
    }

    public Integer getFirstWeek() {
        return firstWeek;
    }

    public void setFirstWeek(Integer firstWeek) {
        this.firstWeek = firstWeek;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getVaccinationGradeNumber() {
        return vaccinationGradeNumber;
    }

    public void setVaccinationGradeNumber(Integer vaccinationGradeNumber) {
        this.vaccinationGradeNumber = vaccinationGradeNumber;
    }

    public Integer getVaccinationPeopleNumber() {
        return vaccinationPeopleNumber;
    }

    public void setVaccinationPeopleNumber(Integer vaccinationPeopleNumber) {
        this.vaccinationPeopleNumber = vaccinationPeopleNumber;
    }

    public Integer getSuspendGradeNumber() {
        return suspendGradeNumber;
    }

    public void setSuspendGradeNumber(Integer suspendGradeNumber) {
        this.suspendGradeNumber = suspendGradeNumber;
    }

    public Integer getSuspendPeopleNumber() {
        return suspendPeopleNumber;
    }

    public void setSuspendPeopleNumber(Integer suspendPeopleNumber) {
        this.suspendPeopleNumber = suspendPeopleNumber;
    }

    public Date getSuspendData() {
        return suspendData;
    }

    public void setSuspendData(Date suspendData) {
        this.suspendData = suspendData;
    }

    public Integer getMunicipalLeveNumber() {
        return municipalLeveNumber;
    }

    public void setMunicipalLeveNumber(Integer municipalLeveNumber) {
        this.municipalLeveNumber = municipalLeveNumber;
    }

    public Integer getDistrictTownNumber() {
        return districtTownNumber;
    }

    public void setDistrictTownNumber(Integer districtTownNumber) {
        this.districtTownNumber = districtTownNumber;
    }

    public Integer getIsEarlyWarningValue() {
        return isEarlyWarningValue;
    }

    public void setIsEarlyWarningValue(Integer isEarlyWarningValue) {
        this.isEarlyWarningValue = isEarlyWarningValue;
    }

    public Integer getIsReasonTraceability() {
        return isReasonTraceability;
    }

    public void setIsReasonTraceability(Integer isReasonTraceability) {
        this.isReasonTraceability = isReasonTraceability;
    }

    public Integer getIsTraceabilityClear() {
        return isTraceabilityClear;
    }

    public void setIsTraceabilityClear(Integer isTraceabilityClear) {
        this.isTraceabilityClear = isTraceabilityClear;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks == null ? null : remarks.trim();
    }
}