package com.paladin.qos.model.school;

import com.paladin.framework.common.BaseModel;

import javax.persistence.Id;

public class OrgSchool extends BaseModel {

    public static final String PARENT_SCHOOL_ID ="parentSchoolId";
	// 
	@Id
	private String id;

	// 主校ID
	private String parentSchoolId;

	// 地址
	private String address;

	// 隶属关系
	private Integer affiliation;

	// 区域
	private String district;

	// 性质
	private Integer nature;

	// 主要领导姓名
	private String mainLeaderName;

	// 主要领导电话
	private String mainLeaderCellphone;

	// 分管领导姓名
	private String chargeLeaderName;

	// 分管领导电话
	private String chargeLeaderCellphone;

	// 具体负责人姓名
	private String specificChargeLeaderName;

	// 具体负责人电话
	private String specificChargeLeaderCellphone;
	
	// 后勤保障人数
	private Integer logistics;
		
	// 校医(兼职)人数
	private Integer schoolDoctorPart;
				
	// 保健老师(兼职)人数
	private Integer healthTeacherPart;

	// 校医(专职)人数
	private Integer schoolDoctorFull;
					
	// 保健老师(专职)人数
	private Integer healthTeacherFull;
	
	//学生总人数
	private Integer total;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getParentSchoolId() {
		return parentSchoolId;
	}

	public void setParentSchoolId(String parentSchoolId) {
		this.parentSchoolId = parentSchoolId;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Integer getAffiliation() {
		return affiliation;
	}

	public void setAffiliation(Integer affiliation) {
		this.affiliation = affiliation;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public Integer getNature() {
		return nature;
	}

	public void setNature(Integer nature) {
		this.nature = nature;
	}

	public String getMainLeaderName() {
		return mainLeaderName;
	}

	public void setMainLeaderName(String mainLeaderName) {
		this.mainLeaderName = mainLeaderName;
	}

	public String getMainLeaderCellphone() {
		return mainLeaderCellphone;
	}

	public void setMainLeaderCellphone(String mainLeaderCellphone) {
		this.mainLeaderCellphone = mainLeaderCellphone;
	}

	public String getChargeLeaderName() {
		return chargeLeaderName;
	}

	public void setChargeLeaderName(String chargeLeaderName) {
		this.chargeLeaderName = chargeLeaderName;
	}

	public String getChargeLeaderCellphone() {
		return chargeLeaderCellphone;
	}

	public void setChargeLeaderCellphone(String chargeLeaderCellphone) {
		this.chargeLeaderCellphone = chargeLeaderCellphone;
	}

	public String getSpecificChargeLeaderName() {
		return specificChargeLeaderName;
	}

	public void setSpecificChargeLeaderName(String specificChargeLeaderName) {
		this.specificChargeLeaderName = specificChargeLeaderName;
	}

	public String getSpecificChargeLeaderCellphone() {
		return specificChargeLeaderCellphone;
	}

	public void setSpecificChargeLeaderCellphone(String specificChargeLeaderCellphone) {
		this.specificChargeLeaderCellphone = specificChargeLeaderCellphone;
	}

	public Integer getLogistics() {
	    return logistics;
	}

	public void setLogistics(Integer logistics) {
	    this.logistics = logistics;
	}

	public Integer getSchoolDoctorPart() {
	    return schoolDoctorPart;
	}

	public void setSchoolDoctorPart(Integer schoolDoctorPart) {
	    this.schoolDoctorPart = schoolDoctorPart;
	}

	public Integer getHealthTeacherPart() {
	    return healthTeacherPart;
	}

	public void setHealthTeacherPart(Integer healthTeacherPart) {
	    this.healthTeacherPart = healthTeacherPart;
	}

	public Integer getSchoolDoctorFull() {
	    return schoolDoctorFull;
	}

	public void setSchoolDoctorFull(Integer schoolDoctorFull) {
	    this.schoolDoctorFull = schoolDoctorFull;
	}

	public Integer getHealthTeacherFull() {
	    return healthTeacherFull;
	}

	public void setHealthTeacherFull(Integer healthTeacherFull) {
	    this.healthTeacherFull = healthTeacherFull;
	}

	public Integer getTotal() {
	    return total;
	}

	public void setTotal(Integer total) {
	    this.total = total;
	}

}