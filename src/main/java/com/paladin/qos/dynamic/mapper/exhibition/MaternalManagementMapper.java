package com.paladin.qos.dynamic.mapper.exhibition;

import java.util.Date;

import org.apache.ibatis.annotations.Param;

/**
 * <p>功能描述</p>：孕产妇管理
 *
 * @author Huangguochen
 * @create 2019/9/3 15:18
 */
public interface MaternalManagementMapper {

    long getMalePremaritalCheckTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    long getFemalePremaritalCheckTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    long getPrepregnancyCheckTotal(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    long getMaternalScreeningTotal(@Param("date") Date date);

    long getHighriskMaternalTotal(@Param("date") Date date);

    long getMaternalDeathTotal(@Param("date") Date date);

    long getCervicalCancerScreeningTotal(@Param("date") Date date);

    long getBreastCancerScreeningTotal(@Param("date") Date date);

    long getSyphilisTestTotal(@Param("date") Date date);

    long getSyphilisInfectionTotal(@Param("date") Date date);

    long getHepatitisBtestTotal(@Param("date") Date date);

    long getHepatitisBdeterminesInfectionTotal(@Param("date") Date date);

    long getWomenDiseaseScreeningTotal(@Param("date") Date date);

    long getPostpartumVisitTotal(@Param("date") Date date);

    long getNumberOfFolates(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    long getFolicAcidDispensingBottle(@Param("startTime") Date startTime, @Param("endTime") Date endTime);

    long getPregnantWomenBuildCardTotal(@Param("date") Date date);

    long getEarlyPregnancyCardTotal(@Param("date") Date date);

    long getMalePremaritalCheckNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getFemalePremaritalCheckNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getPrepregnancyCheckNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getMaternalScreeningNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getHighriskMaternalNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getMaternalDeathNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getCervicalCancerScreeningNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getBreastCancerScreeningNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getSyphilisTestNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getSyphilisInfectionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getHepatitisBtestNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getHepatitisBdeterminesInfectionNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getWomenDiseaseScreeningNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getMaternalVisitNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getNewbornVisitNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getFirstExaminationTimesNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getCycleExaminationTimesNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getNumberOfFolatesNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getFolicAcidDispensingBottleNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getPregnantWomenBuildCardNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

    long getEarlyPregnancyCardNumber(@Param("startTime") Date startTime, @Param("endTime") Date endTime, @Param("code") String code);

}
