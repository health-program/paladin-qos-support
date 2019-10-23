package com.paladin.qos.dynamic.mapper.yiyuan.opd;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

/**   
 * @author MyKite
 * @version 2019年9月17日 上午10:03:43 
 */
public interface OpdStatisticsMapper {
    
    /**急诊人次*/
    public long emergencyNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime);
    
    /**门诊人次*/
    public long outpatientNum(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    /**门急诊总人次*/
    public long OPDTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    
    /**门急诊使用药品数*/
    public long OPDDrugxyTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    public long OPDDrugzyTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    
    /**门急诊药品总费用(元)*/
    public long OPDMoneyTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    /**住院人次*/
    public long HospitalizationNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    
    /**住院患者使用药品数*/
    public long HospitalizationDrugTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);
    
    /**住院药品总费用(元)*/
    public long HospitalizationMoneyTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("unitId") String unitId);

    /**实际开放床日数*/
    public long publicBedDayTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    /**实际占用床日数*/
    public long useBedDayTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime);
    
    /**支付方式-医保 */
    public long paymentMethodMedical(@Param("startTime") Date startTime, @Param("endTime") Date endTime);
    
    /**支付方式-自费 */
    public long paymentMethodOwn(@Param("startTime") Date startTime, @Param("endTime") Date endTime);
    
    /**药品收入 */
    public long drugIncome(@Param("startTime") Date startTime, @Param("endTime") Date endTime);
    
    /**其他收入 */
    public long otherIncome(@Param("startTime") Date startTime, @Param("endTime") Date endTime);
    
    /**医疗收入 */
    public long medicalIncome(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    /**预约挂号数量*/
	public long getAppointmentNum(Date startTime, Date endTime);

	/**总诊疗人次数*/
	public long getTreatmentNum(Date startTime, Date endTime);

	/**检查人次数*/
	public long getCheckNum(Date startTime, Date endTime);
    /**检验人次数*/
	public long getTestNum(Date startTime, Date endTime);

	public long getTotaldays(Date startTime, Date endTime);

	public long getTotalPeople(Date startTime, Date endTime);

}
