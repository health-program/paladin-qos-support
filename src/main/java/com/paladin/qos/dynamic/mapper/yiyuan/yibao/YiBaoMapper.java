package com.paladin.qos.dynamic.mapper.yiyuan.yibao;

import java.util.Map;

public interface YiBaoMapper {

	//诊察
	double getZhenChaCost(Map<String, Object> params);
	//检查
	double getJianChaCost(Map<String, Object> params);
	//化验
	double getHuaYanCost(Map<String, Object> params);
	//治疗
	double getZhiLiaoCost(Map<String, Object> params);
	//手术
	double getShouShuCost(Map<String, Object> params);
	//材料
	double getCaiLiaoCost(Map<String, Object> params);
	//西药
	double getXiYaoCost(Map<String, Object> params);
	//中成药
	double getZhongChengYaoCost(Map<String, Object> params);
	//中草药
	double getZhongCaoYaoCost(Map<String, Object> params);
	//服务
	double getFuWuCost(Map<String, Object> params);
	//其他
	double getQiTaCost(Map<String, Object> params);
	//药品
	double getYaoPinCost(Map<String, Object> params);
	//门诊医保个人负担金额
	double getPersonalCost(Map<String, Object> params);
}
