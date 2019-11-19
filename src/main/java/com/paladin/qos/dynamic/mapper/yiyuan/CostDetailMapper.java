package com.paladin.qos.dynamic.mapper.yiyuan;

import java.util.Map;

public interface CostDetailMapper {

	//总收费
	double getTotalCost(Map<String, Object> params);
	//耗材费用
	double getProduceCost(Map<String, Object> params);
	//手术费用
	double getOperationCost(Map<String, Object> params);
	//门诊医保费用
	double getOutpatientCost(Map<String, Object> params);
}
