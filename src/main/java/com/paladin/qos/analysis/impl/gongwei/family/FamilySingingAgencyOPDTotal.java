package com.paladin.qos.analysis.impl.gongwei.family;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.pagehelper.util.StringUtil;
import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataProcessUnit;
import com.paladin.qos.analysis.impl.gongwei.GongWeiDataProcessor;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.dynamic.mapper.familydoctor.DataFamilyDoctorMapper;
import com.paladin.qos.dynamic.mapper.familydoctor.DataFamilyVO;

/**
 * 签约机构门诊就诊率
 * 
 * @author MyKite
 * @version 2019年9月11日 上午11:10:46
 */
@Component
public class FamilySingingAgencyOPDTotal extends GongWeiDataProcessor {

	@Autowired
	private SqlSessionContainer sqlSessionContainer;

	public static final String EVENT_ID = "21003";

	@Override
	public String getEventId() {
		return EVENT_ID;
	}

	@Override
	public long getTotalNum(Date startTime, Date endTime, String unitId) {
	    long tatol = 0;
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		
		String unit1 = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(unit1)) {
			return 0;
		}
		
		List<String> singingAgencyOPDTotal = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class)
				.singingAgencyOPDTotal(startTime, endTime, unit1);
		
		if (singingAgencyOPDTotal != null && singingAgencyOPDTotal.size() > 0) {
		    
		    List<String> registerOPDtotal1 = new ArrayList<String>();
		    
		    if (singingAgencyOPDTotal != null && singingAgencyOPDTotal.size() > 0) {
			int listSize = singingAgencyOPDTotal.size();
			int toIndex = 1000;
			for (int i = 0; i < singingAgencyOPDTotal.size(); i += toIndex) {
				if (i + toIndex > listSize) {
					toIndex = listSize - i;
				}
				List<String> newList = singingAgencyOPDTotal.subList(i, i + toIndex);
				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
				List<String> registerOPDtotal = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPDtotal(startTime,
						endTime, unitId, newList);
				registerOPDtotal1.addAll(registerOPDtotal);
			}
		}
			
			if (registerOPDtotal1 != null && registerOPDtotal1.size() > 0) {
			    int listSize1 = registerOPDtotal1.size();
			    int toIndex1 = 1000;
			    List<DataProcessUnit> units =DataConstantContainer.getHospitalList();
				 for (DataProcessUnit u : units) {
				     String dbCode = u.getSource().getDbCode();
					if (dbCode != null && dbCode.length() > 0) {
					    
					    for (int i = 0; i < singingAgencyOPDTotal.size(); i += toIndex1) {
						if (i + toIndex1 > listSize1) {
							toIndex1 = listSize1 - i;
						}
						List<String> newList1 = singingAgencyOPDTotal.subList(i, i + toIndex1);
						sqlSessionContainer.setCurrentDataSource(dbCode);
						tatol += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).hospitalOPDTotal(startTime, endTime, unitId,
							newList1);
					    }
					    
					}
				}
			}
		}
		return tatol;
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
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
				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_YIYUAN);
				tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPD(startTime, endTime, newList);
			}
		}
		return tatal;
	}
}
