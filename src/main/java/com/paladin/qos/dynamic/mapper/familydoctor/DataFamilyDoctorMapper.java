package com.paladin.qos.dynamic.mapper.familydoctor;

import java.util.Date;
import java.util.List;

import org.apache.ibatis.annotations.Param;

/**
 * @author MyKite
 * @version 2019年9月5日 下午4:51:04
 */
public interface DataFamilyDoctorMapper {

	/**
	 * 综合健康管理服务包签约率（收费）
	 */
	public long signingIsMoneyNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/**
	 * 综合健康管理服务包签约率（免费）
	 */
	public long signingNotMoneyNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	
	public long singingResidentNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);// 应签约居民数

	/**
	 * 个性化服务签约率（收费）
	 */
	public long singingServeNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);// 签约个性化服务数

	public long singingTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);// 签约总数

	/** 签约机构门诊就诊率 */
	// 签约居民中心就诊人次数
	public List<String> singingAgencyOPDpersonNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	public long registerOPD(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId,
			@Param("list") List<String> idcards);

	// 签约居民就诊总次数
	public List<String> singingAgencyOPDTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	public List<String> registerOPDtotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId,
			@Param("idCard") List<String> idCard);

	public long registerTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime,
								 @Param("idCard") List<String> idCard);

	public long hospitalOPDTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime,
			@Param("idCard") List<String> idCard);

	/** 签约医生门诊就诊率 */
	public List<String> singingDoctorOPDtotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);// 签约居民家庭医生就诊总数

	public long docnameOPDnum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId,
			@Param("list") List<String> idcards);

	/** 慢病签约人员管理数 */
	public long singingPersonManageNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 慢病签约人员随访数 */
	public long singingPersonFollowNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 上门服务人次数 */
	public long doorServicePersonNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 营养指导人次数 */
	public long nutritionGuidePersonNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 康复指导人次数 */
	public long recoveryGuidePersonNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 用药指导人次数 */
	public long medicationGuidePersonNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 慢病长处方服务数 */
	public long prescriptionServicNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 签约居民体检人数 */
	public long singingInspectNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 心脑血管高危人群筛查人数 */
	public long cardiovascularSiftNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 肿瘤高危人群筛查数 */
	public long tumourSiftNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 骨质疏松高危人群筛查数 */
	public long osteoporosisSiftNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** COPD高危人群筛查数 */
	public long copdSiftNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 儿童哮喘筛查数 */
	public long childhoodSsthmaSiftNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 健康自理检测评估数 */
	public long healthyselfcareSssessNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** CDSS心脑血管风险评估数 */
	public long cdssSssessNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 个性化健康信息精准推送数 */
	public long personalizedHealthPushNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 家庭医生签约服务协议表总数 */
	public long singingServiceTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 残疾人签约率 */
	public long disabledSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 高血压患者签约率 */
	public long hypertensionSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 糖尿病患者签约率 */
	public long diabetesSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 孕产妇签约率 */
	public long maternalSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 0-6周岁儿童签约率 */
	public long childrenSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 65周岁及以上老年人签约率 */
	public long agedSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 严重肺结核患者签约率 */
	public long tuberculosisSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 重精神障碍患者签约率 */
	public long mentalDisorderSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	/** 疾病筛查高危人员签约率 */
	public long diseaseSiftSigningRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	
	/** 离休干部签约率 */
	public long retirementCadreRate(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	
}
