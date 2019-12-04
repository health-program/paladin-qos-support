package com.paladin.qos.service.data.dto;

import java.util.Date;

/**
 * @author TontoZhou
 * @since 2019/12/3
 */
public class ReadHospitalData {

    private Long amount;
    private Date dayTime;

    public Date getDayTime() {
        return dayTime;
    }

    public void setDayTime(Date dayTime) {
        this.dayTime = dayTime;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }
}
