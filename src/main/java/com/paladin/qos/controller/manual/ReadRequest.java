package com.paladin.qos.controller.manual;

import com.paladin.qos.service.data.dto.ReadHospitalDataEvent;

/**
 * @author TontoZhou
 * @since 2019/12/3
 */
public class ReadRequest extends ReadHospitalDataEvent {

    private String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

}
