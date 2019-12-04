package com.paladin.qos.service.data.dto;

import java.util.Date;
import java.util.List;

/**
 * @author TontoZhou
 * @since 2019/12/3
 */
public class ReadHospitalDataEvent {

    private String unitId;
    private String eventId;
    private String type;
    private Date startTime;
    private Date endTime;

    private List<ReadHospitalData> datas;

    public String getUnitId() {
        return unitId;
    }

    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public List<ReadHospitalData> getDatas() {
        return datas;
    }

    public void setDatas(List<ReadHospitalData> datas) {
        this.datas = datas;
    }
}
