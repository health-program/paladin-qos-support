package com.paladin.qos.dynamic.mapper.shejike;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

public interface SheJiKeMapper {
	// 医生平均门急诊量
	long getAverageNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 使用床位数
	long getBedInUseNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 急诊人次数
	long getEmergencyNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 住院人次数
	long getInhospitalNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	// 住院人次数（医保）
	long getInhospitalNumberMedicare(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	// 住院人次数（自费）
	long getInhospitalNumberSelf(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	// 在院人次数
	long getOnhospitalNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 出院人次数
	long getOuthospitalNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	// 出院人次数（医保）
	long getOuthospitalNumberMedicare(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	// 出院人次数（自费）
	long getOuthospitalNumberSelf(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	// 门诊人次数
	long getOutpatientNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 门急诊总数
	long getPatientsNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 额定床位数
	long getRatedBedNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 出诊医生数
	long getVisitDoctorNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 病床总天数
	long getBedTotalDays(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 病床使用天数
	long getBedInUseDays(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 合计总收入
	double getTotalMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 医疗收入
	double getMedicalMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 药品收入
	double getDrugMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 其他收入
	double getOtherMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 处方数量
	long getPrescriptionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 处方总额
	double getPrescriptionMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 最大处方金额
	double getMaxPrescriptionMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 最小处方金额
	double getMinPrescriptionMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 平均处方金额
	double getAvgPrescriptionMoney(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 门诊中医处方占比
	long getTotalChineseMedicinePrescription(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getEventChineseMedicinePrescription(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 门诊中药饮片占比
	long getEventChineseDrinkMedicine(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 门诊中医非药物治疗处方占比
	long getEventNonChineseMedicinePrescription(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 激素使用率
	long getTotalHormone(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	long getEventHormone(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 抗菌药物使用率
	double getTotalAntiDrug(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	double getEventAntiDrug(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 一联抗生素使用率
	long getTotalRecipe(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	//静脉输液率
	long getEventShuyeLv(Date startTime, Date endTime, String unitId);
	
	long getEventOneAntiDrug(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 二联抗生素使用率
	long getEventTwoAntiDrug(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	// 三联抗生素使用率
	long getEventThreeAntiDrug(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

	//医保挂号人数
	long getYiPay(Date startTime, Date endTime, String unitId);

	//自费挂号人数
	long getSelfPay(Date startTime, Date endTime, String unitId);

	//检查人次数
	long getCheckNumber(Date startTime, Date endTime, String unitId);
	//检查人次数（窗口）
	long getCheckNumberWin(Date startTime, Date endTime, String unitId);
	//检查人次数（自助）
	long getCheckNumberSelf(Date startTime, Date endTime, String unitId);

	//检验人次数
	long getTestNumber(Date startTime, Date endTime, String unitId);
	//检验人次数
	long getTestNumberWin(Date startTime, Date endTime, String unitId);
	//检验人次数
	long getTestNumberSelf(Date startTime, Date endTime, String unitId);
	
	long getPatientsNumberMedicare(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberSelf(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberFirst(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberSecond(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberWindowPre(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberPhone(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberWindow(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberFee(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberStation(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	long getPatientsNumberMachine(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	
	
	double getTotalMoneyMedicare(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneySelf(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyFirst(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneySecond(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyWindowPre(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyPhone(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyWindow(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyFee(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyStation(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
	double getTotalMoneyMachine(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
}
