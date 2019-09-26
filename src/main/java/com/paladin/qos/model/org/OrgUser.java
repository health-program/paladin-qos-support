package com.paladin.qos.model.org;

import com.paladin.framework.common.BaseModel;
import javax.persistence.Id;

public class OrgUser extends BaseModel {

	public final static String COLUMN_FIELD_IDENTIFICATION_ID = "identificationId";

	// 
	@Id
	private String id;

	// 
	private String account;

	// 医院
	private String agencyId;

	// 角色
	private String roleId;

	// 名称
	private String name;

	// 手机
	private String cellphone;

	// 身份证
	private String identificationId;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getRoleId() {
		return roleId;
	}

	public void setRoleId(String roleId) {
		this.roleId = roleId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCellphone() {
		return cellphone;
	}

	public void setCellphone(String cellphone) {
		this.cellphone = cellphone;
	}

	public String getIdentificationId() {
		return identificationId;
	}

	public void setIdentificationId(String identificationId) {
		this.identificationId = identificationId;
	}

	public String getAgencyId() {
		return agencyId;
	}

	public void setAgencyId(String agencyId) {
		this.agencyId = agencyId;
	}

}