package com.paladin.qos.analysis;

import com.paladin.framework.spring.SpringBeanHelper;
import com.paladin.framework.spring.SpringContainer;
import com.paladin.qos.core.DataTask;
import com.paladin.qos.core.DataTaskManager;
import com.paladin.qos.dynamic.DSConstant;
import com.paladin.qos.model.data.DataEvent;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.analysis.AnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.Map.Entry;

@Component
public class DataProcessContainer implements SpringContainer {

    private static Logger logger = LoggerFactory.getLogger(DataProcessContainer.class);

    private Map<String, DataProcessor> processorMap = new HashMap<>();

    @Value("${qos.start-repair-thread}")
    private boolean startRepairThread = false;

    @Autowired
    private AnalysisService analysisService;

    @Autowired
    private DataTaskManager dataTaskManager;

    @Override
    public boolean initialize() {

        Map<String, DataProcessor> processorSpringMap = SpringBeanHelper.getBeansByType(DataProcessor.class);
        Map<String, DataProcessor> processorMap = new HashMap<>();

        for (Entry<String, DataProcessor> entry : processorSpringMap.entrySet()) {

            DataProcessor processor = entry.getValue();

            String eventId = processor.getEventId();

            DataEvent dataEvent = DataConstantContainer.getEvent(eventId);

            if (dataEvent == null) {
                logger.error("找不到处理器对应事件：" + eventId);
                continue;
            }

            List<DataUnit> units = null;
            int targetType = dataEvent.getTargetType();

            if (targetType == DataEvent.TARGET_TYPE_ALL) {
                units = DataConstantContainer.getUnitList();
            } else if (targetType == DataEvent.TARGET_TYPE_HOSPITAL) {
                units = DataConstantContainer.getHospitalList();
            } else if (targetType == DataEvent.TARGET_TYPE_COMMUNITY) {
                units = DataConstantContainer.getCommunityList();
            }

            if (dataEvent.getDataSource().equals(DSConstant.DS_YIYUAN)) {
                // 因为医院数据库分开，打乱执行医院顺序从而实现一定的负载均衡；
                List<DataUnit> randomUnits = new ArrayList<>(units);
                Collections.shuffle(randomUnits);
                processor.setTargetUnits(randomUnits);
            } else {
                processor.setTargetUnits(units);
            }

            processor.setDataEvent(dataEvent);

            if (processorMap.containsKey(eventId)) {
                logger.warn("===>已经存在数据预处理器[EVENT_ID:" + eventId + "]，该处理器会被忽略");
                continue;
            }

            processorMap.put(eventId, processor);
        }

        this.processorMap = Collections.unmodifiableMap(processorMap);

        registerTask();

        return true;
    }

    private void registerTask() {

        List<DataTask> realTimeTasks = new ArrayList<>();
        List<DataTask> nightTasks = new ArrayList<>();

        for (DataProcessor processor : processorMap.values()) {

            DataEvent dataEvent = processor.getDataEvent();
            DataProcessTask task = new DataProcessTask(processor, analysisService);

            if (dataEvent.getRealTimeEnabled() == 1) {
                realTimeTasks.add(task);
            } else {
                nightTasks.add(task);
            }

            // 注册修复任务
            if (startRepairThread) {
                DataProcessRepairTask repairTask = new DataProcessRepairTask(processor, analysisService);
                nightTasks.add(repairTask);
            }
        }

        dataTaskManager.registerTaskSchedule(nightTasks);
        dataTaskManager.registerTaskRealTime(realTimeTasks);
    }

    /**
     * 获取数据处理器
     *
     * @param eventId
     * @return
     */
    public DataProcessor getDataProcessor(String eventId) {
        return processorMap.get(eventId);
    }

    /**
     * 获取数据处理器集合
     *
     * @return
     */
    public Collection<DataProcessor> getDataProcessors() {
        return processorMap.values();
    }

    public int order() {
        return 1;
    }

}
