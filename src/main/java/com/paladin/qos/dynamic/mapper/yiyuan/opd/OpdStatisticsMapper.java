package com.paladin.qos.dynamic.mapper.yiyuan.opd;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

/**   
 * @author MyKite
 * @version 2019年9月17日 上午10:03:43 
 */
public interface OpdStatisticsMapper {

    /**门诊总人次*/
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
    

}
