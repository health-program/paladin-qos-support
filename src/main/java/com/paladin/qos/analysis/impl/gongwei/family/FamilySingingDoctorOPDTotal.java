package com.paladin.qos.analysis.impl.gongwei.family;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.pagehelper.util.StringUtil;
import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.gongwei.GongWeiDataProcessor;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.dynamic.mapper.familydoctor.DataFamilyDoctorMapper;

/**
 * 签约医生门诊就诊率
 * 
 * @author MyKite
 * @version 2019年9月11日 上午11:12:23
 */
@Component
public class FamilySingingDoctorOPDTotal extends GongWeiDataProcessor {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "21004";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		String gongweiUnitId = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(gongweiUnitId)) {
			return 0;
		}

		List<String> idcards = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingAgencyOPDpersonNum(startTime, endTime,
				gongweiUnitId);
		
		//过滤idcards中的空值或空字符串
		List<String> newidCards = new ArrayList<String>();
		for(String idcard:idcards){
			if(StringUtil.isNotEmpty(idcard)){
				newidCards.add(idcard);
			}
		}
		
		
		long tatal = 0;
		if (newidCards != null && newidCards.size() > 0) {
			int listSize = newidCards.size();
			for (int i = 0, j = 0; i < listSize; i = j) {
				j += 500;

				if (j > listSize) {
					j = listSize;
				}

				List<String> newList = newidCards.subList(i, j);

				if (newList.size() == 0) {
					break;
				}

				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
				tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPD(startTime, endTime, unitId, newList);
			}
		}
		return tatal;
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		long tatal = 0;

		String gongweiUnitId = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(gongweiUnitId)) {
			return 0;
		}

		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		List<String> vo = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingDoctorOPDtotal(startTime, endTime,
				gongweiUnitId);
		
		//过滤vo中的空值或空字符串
		List<String> newidCards = new ArrayList<String>();
		for(String idcard:vo){
			if(StringUtil.isNotEmpty(idcard)){
				newidCards.add(idcard);
			}
		}

		if (newidCards != null && newidCards.size() > 0) {
			int listSize = newidCards.size();

			for (int i = 0, j = 0; i < listSize; i = j) {
				j += 500;

				if (j > listSize) {
					j = listSize;
				}

				List<String> newList = newidCards.subList(i, j);

				if (newList.size() == 0) {
					break;
				}

				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
				tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).docnameOPDnum(startTime, endTime, unitId, newList);
			}
		}
		return tatal;
	}

}
