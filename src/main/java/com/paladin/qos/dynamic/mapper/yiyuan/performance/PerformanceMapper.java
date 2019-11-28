package com.paladin.qos.dynamic.mapper.yiyuan.performance;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

/**   
 * @author MyKite
 * @version 2019年9月17日 上午10:03:43 
 */
public interface PerformanceMapper {
    
	/**门诊医保费用 */
	public long getOutpatientMedicare(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**门诊总费用 */
	public long getOutpatientFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**住院医保费用 */
	public long getInhospitableMedicare(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**住院总费用 */
	public long getInhospitableFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**总费用 */
	public long getTotalFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**医疗服务收入 */
	public long getMedicalServiceFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**药品收入 */
	public long getDrugFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**辅药收入 */
	public long getAssistDrug(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**门诊人次 */
	public long getPatientNum(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**出院患者关联的药品总费用 */
	public long getInhospitableDrugFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**出院患者关联的住院总费用 */
	public long getOuthospitableTotalFee(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	/**总床日数 */
	public long getTotalBedDay(@Param("startTime")Date startTime, @Param("endTime") Date  endTime);
	

	


}
