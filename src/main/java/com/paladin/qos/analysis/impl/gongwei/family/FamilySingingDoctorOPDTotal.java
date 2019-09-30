package com.paladin.qos.analysis.impl.gongwei.family;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.pagehelper.util.StringUtil;
import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.impl.gongwei.GongWeiDataProcessor;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.dynamic.mapper.familydoctor.DataFamilyDoctorMapper;
import com.paladin.qos.dynamic.mapper.familydoctor.DataFamilyVO;

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
		String unit1 = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(unit1)) {
			return 0;
		}
		List<DataFamilyVO> vo = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingAgencyOPDpersonNum(startTime,
				endTime, unit1);
		
		for (DataFamilyVO v : vo) {
		    if(unit1.equals(v.getUnitId())){
			v.setUnitId(unitId);
		    }
		}
		
		long tatal = 0;
		if (vo != null && vo.size() > 0) {
			int listSize = vo.size();
			int toIndex = 1000;
			for (int i = 0; i < vo.size(); i += toIndex) {
				if (i + toIndex > listSize) {
					toIndex = listSize - i;
				}
				List<DataFamilyVO> newList = vo.subList(i, i + toIndex);
				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
				tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPD(startTime, endTime, newList);
			}
		}
		return tatal;
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
	    long tatal = 0;
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		List<DataFamilyVO> vo = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingDoctorOPDtotal(startTime, endTime,
				unitId);
		
		String unit1 = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(unit1)) {
			return 0;
		}
		
		for (DataFamilyVO v : vo) {
		    if(unit1.equals(v.getUnitId())){
			v.setUnitId(unitId);
		    }
		}
		
		if (vo != null && vo.size() > 0) {
			for (DataFamilyVO d : vo) {
				String unit = getMappingUnitId(d.getUnitId());
				if (unit != null) {
					d.setUnitId(getMappingUnitId(d.getUnitId()));
				}
			}
			sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
			tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).docnameOPDnum(startTime, endTime, vo);
		}
		return tatal;
	}
}
