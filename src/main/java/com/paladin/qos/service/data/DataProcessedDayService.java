package com.paladin.qos.service.data;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.core.ServiceSupport;
import com.paladin.framework.core.exception.BusinessException;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.dynamic.mapper.core.DataViewCreateMapper;
import com.paladin.qos.mapper.data.DataProcessedDayMapper;
import com.paladin.qos.model.data.DataProcessedDay;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.data.dto.ReadHospitalData;
import com.paladin.qos.service.data.dto.ReadHospitalDataEvent;
import com.paladin.qos.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class DataProcessedDayService extends ServiceSupport<DataProcessedDay> {

    private static Logger logger = LoggerFactory.getLogger(DataProcessedDayService.class);

    @Autowired
    private DataProcessedDayMapper dataProcessedDayMapper;

    @Autowired
    protected SqlSessionContainer sqlSessionContainer;

    public boolean updateOrSave(DataProcessedDay model) {
        if (dataProcessedDayMapper.updateData(model) == 0) {
            if (dataProcessedDayMapper.insert(model) == 0) {
                return false;
            }
        }
        return true;
    }

    public void saveDataByManual(ReadHospitalDataEvent readEvent) {

        String eventId = readEvent.getEventId();
        String unitId = readEvent.getUnitId();
        String type = readEvent.getType();
        Date startTime = readEvent.getStartTime();
        Date endTime = readEvent.getEndTime();

        List<ReadHospitalData> datas = readEvent.getDatas();

        int lastSN = TimeUtil.getSerialNumberByDay(endTime);

        List<Integer> serialNumbers = TimeUtil.getSerialNumberByDay(startTime, endTime);
        Map<Integer, Integer> finishedMap = new HashMap<>();
        for (Integer sn : serialNumbers) {
            finishedMap.put(sn, 0);
        }

        DataProcessedDay model = new DataProcessedDay();
        model.setEventId(eventId);

        DataUnit unit = DataConstantContainer.getUnit(unitId);
        if (unit == null) {
            throw new BusinessException("不存在机构[" + unitId + "]");
        }

        model.setUnitId(unitId);
        model.setUnitType(unit.getType());
        model.setConfirmed(1);

        for (ReadHospitalData data : datas) {
            Long amount = data.getAmount();
            Date dayTime = data.getDayTime();

            Calendar c = Calendar.getInstance();
            c.setTime(dayTime);

            int year = c.get(Calendar.YEAR);
            int month = c.get(Calendar.MONTH) + 1;
            int day = c.get(Calendar.DAY_OF_MONTH);
            int weekYear = c.get(Calendar.WEEK_OF_YEAR);
            int weekMonth = c.get(Calendar.WEEK_OF_MONTH);

            StringBuilder sb = new StringBuilder(eventId);
            sb.append('_').append(unitId).append('_');
            sb.append(year);
            if (month < 10) {
                sb.append('0');
            }
            sb.append(month);
            if (day < 10) {
                sb.append('0');
            }
            sb.append(day);
            String id = sb.toString();

            model.setId(id);
            model.setEventId(eventId);
            model.setDay(day);
            model.setMonth(month);
            model.setYear(year);
            model.setWeekMonth(weekMonth);
            model.setWeekYear(weekYear);

            int serialNumber = year * 10000 + month * 100 + day;
            if (lastSN < serialNumber) {
                continue;
            }

            model.setSerialNumber(serialNumber);

            DataProcessedDay oldData = get(id);

            if (oldData != null) {
                if ("totalNum".equals(type)) {
                    oldData.setTotalNum(amount);
                } else {
                    oldData.setEventNum(amount);
                }
                oldData.setUpdateTime(new Date());
                update(oldData);
                finishedMap.put(oldData.getSerialNumber(), 2);
            } else {
                if ("totalNum".equals(type)) {
                    model.setTotalNum(amount);
                    model.setEventNum(0L);
                } else {
                    model.setTotalNum(0L);
                    model.setEventNum(amount);
                }

                model.setUpdateTime(new Date());
                save(model);
                finishedMap.put(model.getSerialNumber(), 1);
            }
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

        for (Map.Entry<Integer, Integer> entry : finishedMap.entrySet()) {
            int sn = entry.getKey();
            int status = entry.getValue();

            if (status == 0) {
                Date dayTime = null;
                try {
                    dayTime = format.parse(String.valueOf(sn));
                } catch (ParseException e) {

                }

                Calendar c = Calendar.getInstance();
                c.setTime(dayTime);

                int year = c.get(Calendar.YEAR);
                int month = c.get(Calendar.MONTH) + 1;
                int day = c.get(Calendar.DAY_OF_MONTH);
                int weekYear = c.get(Calendar.WEEK_OF_YEAR);
                int weekMonth = c.get(Calendar.WEEK_OF_MONTH);

                StringBuilder sb = new StringBuilder(eventId);
                sb.append('_').append(unitId).append('_');
                sb.append(year);
                if (month < 10) {
                    sb.append('0');
                }
                sb.append(month);
                if (day < 10) {
                    sb.append('0');
                }
                sb.append(day);
                String id = sb.toString();

                model.setId(id);
                model.setEventId(eventId);
                model.setDay(day);
                model.setMonth(month);
                model.setYear(year);
                model.setWeekMonth(weekMonth);
                model.setWeekYear(weekYear);

                model.setSerialNumber(sn);

                DataProcessedDay oldData = get(id);

                if (oldData == null) {
                    model.setTotalNum(0L);
                    model.setEventNum(0L);
                    model.setUpdateTime(new Date());
                    save(model);
                } else {
                    if ("totalNum".equals(type)) {
                        oldData.setTotalNum(0L);
                    } else {
                        oldData.setEventNum(0L);
                    }
                    oldData.setUpdateTime(new Date());
                    update(oldData);
                }
            }
        }
    }

    public void readHospitalDataBySql(ReadHospitalDataEvent readEvent, String sql) {

        DataUnit hospital = DataConstantContainer.getUnit(readEvent.getUnitId());
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");

        sqlSessionContainer.setCurrentDataSource(hospital.getDbCode());
        DataViewCreateMapper mapper = sqlSessionContainer.getSqlSessionTemplate().getMapper(DataViewCreateMapper.class);
        List<Map<String, Object>> mapResult = mapper.selectView(sql);

        List<ReadHospitalData> datas = new ArrayList<>(mapResult.size());
        for (Map<String, Object> dataMap : mapResult) {
            Object dayTimeObj = dataMap.get("DAYTIME");
            Object amountObj = dataMap.get("AMOUNT");

            Date dayTime = null;
            if (dayTimeObj instanceof Date) {
                dayTime = (Date) dayTimeObj;
            } else {
                try {
                    dayTime = format1.parse(dayTimeObj.toString());
                } catch (ParseException e) {
                    throw new BusinessException("时间格式错误", e);
                }
            }

            Long amount = null;
            if (amountObj instanceof Number) {
                amount = ((Number) amountObj).longValue();
            } else {
                amount = new BigDecimal(amountObj.toString()).longValue();
            }

            ReadHospitalData data = new ReadHospitalData();
            data.setDayTime(dayTime);
            data.setAmount(amount);

            datas.add(data);
        }

        readEvent.setUnitId(hospital.getId());
        readEvent.setDatas(datas);

        saveDataByManual(readEvent);
    }

    public synchronized void readHospitalData(ReadHospitalDataEvent readEvent, String sql) {
        String eventId = readEvent.getEventId();

        if (DataConstantContainer.getEvent(eventId) == null) {
            throw new BusinessException("找不到对应事件[" + eventId + "]");
        }

        ReadHospitalDataThread thread = readDataThreadMap.get(eventId);

        if (thread == null) {
            thread = new ReadHospitalDataThread();
            readDataThreadMap.put(eventId, thread);
        } else if (!thread.finished) {
            throw new BusinessException("该事件已经在运行读取数据任务");
        }

        thread.init(readEvent, sql);

        final ReadHospitalDataThread finalThread = thread;

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    List<DataUnit> hospitals = new ArrayList<>(DataConstantContainer.getHospitalList());
                    Collections.shuffle(hospitals);

                    for (DataUnit hospital : hospitals) {
                        readEvent.setUnitId(hospital.getId());
                        readHospitalDataBySql(readEvent, sql);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finalThread.finished = true;
                    finalThread.endTime = new Date();
                    long executeTime = finalThread.getEndTime().getTime() - finalThread.getStartTime().getTime();
                    logger.info("完成读取事件[" + finalThread.getReadEvent().getEventId() + "]数据任务,耗时：" + executeTime + "毫秒");
                }
            }
        });

        t.start();
    }

    public List<ReadHospitalDataThread> getReadThreadStatus() {
        return new ArrayList<>(readDataThreadMap.values());
    }

    private Map<String, ReadHospitalDataThread> readDataThreadMap = new HashMap<>();

    public static class ReadHospitalDataThread {
        private Date startTime;
        private Date endTime;
        private ReadHospitalDataEvent readEvent;
        private boolean finished;
        private String sql;

        public void init(ReadHospitalDataEvent readEvent, String sql) {
            this.readEvent = readEvent;
            this.finished = false;
            this.startTime = new Date();
            this.sql = sql;
        }

        public ReadHospitalDataEvent getReadEvent() {
            return readEvent;
        }

        public boolean isFinished() {
            return finished;
        }

        public Date getEndTime() {
            return endTime;
        }

        public Date getStartTime() {
            return startTime;
        }

        public String getSql() {
            return sql;
        }
    }

}