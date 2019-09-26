package com.paladin.qos.analysis.impl.gongwei.family;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.analysis.DataConstantContainer.Unit;
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
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		List<DataFamilyVO> vo = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingAgencyOPDpersonNum(startTime,
				endTime, unitId);
		long tatal = 0;
		if (vo != null && vo.size() > 0) {
			int listSize = vo.size();
			int toIndex = 1000;
			for (int i = 0; i < vo.size(); i += toIndex) {
				if (i + toIndex > listSize) {
					toIndex = listSize - i;
				}
				List<DataFamilyVO> newList = vo.subList(i, i + toIndex);
				for (DataFamilyVO d : newList) {
					String unit = getMappingUnitId(d.getUnitId());
					if (unit != null) {
						d.setUnitId(getMappingUnitId(d.getUnitId()));
						d.setIdCard(d.getIdCard());
					}
				}
				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_YIYUAN);
				tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPD(startTime, endTime, newList);
			}
		}
		return tatal;
	}

	@Override
	public long getEventNum(Date startTime, Date endTime, String unitId) {
		long tatol = 0;
		sqlSessionContainer.setCurrentDataSource(DSConstant.DS_GONGWEI);
		List<String> singingAgencyOPDTotal = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class)
				.singingAgencyOPDTotal(startTime, endTime, unitId);
		if (singingAgencyOPDTotal != null && singingAgencyOPDTotal.size() > 0) {
			sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
			List<String> registerOPDtotal = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPDtotal(startTime,
					endTime, unitId, singingAgencyOPDTotal);
			if (registerOPDtotal != null && registerOPDtotal.size() > 0) {
			    
			    List<Unit> units =DataConstantContainer.getHospitalList();
				 for (Unit u : units) {
				     String dbCode = u.getSource().getDbCode();
					if (dbCode != null && dbCode.length() > 0) {
					    sqlSessionContainer.setCurrentDataSource(dbCode);
						tatol += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).hospitalOPDTotal(startTime, endTime, unitId,
								registerOPDtotal);
					}
				}
			}
		}
		return tatol;
	}
}
