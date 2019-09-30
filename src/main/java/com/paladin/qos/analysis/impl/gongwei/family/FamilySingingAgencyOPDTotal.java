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

		String gongweiUnitId = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(gongweiUnitId)) {
			return 0;
		}

		List<String> idcards = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingAgencyOPDTotal(startTime, endTime,
				gongweiUnitId);

		if (idcards != null && idcards.size() > 0) {

			List<String> registerOPDtotal1 = new ArrayList<String>();

			int listSize = idcards.size();
			for (int i = 0, j = 0; i < listSize; i = j) {
				j += 500;
				if (j > listSize) {
					j = listSize;
				}

				List<String> newList = idcards.subList(i, j);
				if (newList.size() == 0) {
					break;
				}

				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
				List<String> registerOPDtotal = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPDtotal(startTime,
						endTime, unitId, newList);
				registerOPDtotal1.addAll(registerOPDtotal);
			}

			if (registerOPDtotal1.size() > 0) {
				listSize = registerOPDtotal1.size();
				
				List<DataProcessUnit> units = DataConstantContainer.getHospitalList();
				for (DataProcessUnit u : units) {
					String dbCode = u.getSource().getDbCode();
					if (dbCode != null && dbCode.length() > 0) {
						for (int i = 0, j = 0; i < listSize; i = j) {
							j += 500;
							if (j > listSize) {
								j = listSize;
							}

							List<String> newList1 = registerOPDtotal1.subList(i, j);
							if (newList1.size() == 0) {
								break;
							}

							sqlSessionContainer.setCurrentDataSource(dbCode);
							tatol += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).hospitalOPDTotal(startTime, endTime,
									unitId, newList1);
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
		String gongweiUnitId = getMappingUnitId(unitId);
		if (StringUtil.isEmpty(gongweiUnitId)) {
			return 0;
		}

		List<String> idcards = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).singingAgencyOPDpersonNum(startTime, endTime,
				gongweiUnitId);

		long tatal = 0;
		if (idcards != null && idcards.size() > 0) {
			int listSize = idcards.size();
			for (int i = 0, j = 0; i < listSize; i = j) {
				j += 500;

				if (j > listSize) {
					j = listSize;
				}

				List<String> newList = idcards.subList(i, j);

				if (newList.size() == 0) {
					break;
				}

				sqlSessionContainer.setCurrentDataSource(DSConstant.DS_JCYL);
				tatal += sqlSessionContainer.getSqlSessionTemplate().getMapper(DataFamilyDoctorMapper.class).registerOPD(startTime, endTime, unitId, newList);
			}
		}
		return tatal;
	}
}
