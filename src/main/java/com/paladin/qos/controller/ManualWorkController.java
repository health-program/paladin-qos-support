package com.paladin.qos.controller;

import com.paladin.framework.common.ExcelImportResult;
import com.paladin.framework.core.exception.BusinessException;
import com.paladin.framework.excel.DefaultSheet;
import com.paladin.framework.excel.read.*;
import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.analysis.DataConstantContainer;
import com.paladin.qos.model.data.DataProcessedDay;
import com.paladin.qos.model.data.DataUnit;
import com.paladin.qos.service.data.DataProcessedDayService;
import com.paladin.qos.util.TimeUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author TontoZhou
 * @since 2019/12/3
 */
@Controller
@RequestMapping("/qos")
public class ManualWorkController {

    @Autowired
    private DataProcessedDayService dataProcessedDayService;

    private static final List<ReadColumn> dataImportColumns = DefaultReadColumn.createReadColumn(ExcelModel.class, null);

    @RequestMapping(value = "/data/import", method = RequestMethod.POST)
    @ResponseBody
    public Object importData(ImportRequest importRequest) {
        XSSFWorkbook workbook;
        try {
            workbook = new XSSFWorkbook(importRequest.getImportFile().getInputStream());
        } catch (IOException e1) {
            throw new BusinessException("导入异常");
        }

        String eventId = importRequest.getEventId();
        String unitId = importRequest.getUnitId();
        String type = importRequest.getType();
        Date startTime = importRequest.getStartTime();
        Date endTime = importRequest.getEndTime();

        int lastSN = TimeUtil.getSerialNumberByDay(endTime);

        List<Integer> serialNumbers = TimeUtil.getSerialNumberByDay(startTime, endTime);
        Map<Integer, Integer> finishedMap = new HashMap<>();
        for (Integer sn : serialNumbers) {
            finishedMap.put(sn, 0);
        }

        ExcelReader<ExcelModel> reader = new ExcelReader<>(ExcelModel.class, dataImportColumns, new DefaultSheet(workbook.getSheetAt(0)), 0);
        List<ExcelImportResult.ExcelImportError> errors = new ArrayList<>();

        DataProcessedDay model = new DataProcessedDay();
        model.setEventId(eventId);

        DataUnit unit = DataConstantContainer.getUnit(unitId);
        if (unit == null) {
            return CommonResponse.getErrorResponse("不存在机构[" + unitId + "]");
        }

        model.setUnitId(unitId);
        model.setUnitType(unit.getType());
        model.setConfirmed(1);

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");

        int i = 0;

        while (reader.hasNext()) {
            i++;
            if (i > 1000) {
                break;
            }

            ExcelModel excelData = null;
            try {
                excelData = reader.readRow();
            } catch (ExcelReadException e) {
                errors.add(new ExcelImportResult.ExcelImportError(i, e));
                continue;
            }

            if (excelData == null) {
                break;
            }

            String amountStr = excelData.getAmount();
            String dayStr = excelData.getDay();

            Date dayTime = null;
            try {
                dayTime = format1.parse(dayStr);
            } catch (ParseException e) {
                errors.add(new ExcelImportResult.ExcelImportError(i, e));
                continue;
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

            int serialNumber = year * 10000 + month * 100 + day;
            if (lastSN < serialNumber) {
                continue;
            }

            model.setSerialNumber(serialNumber);
            Long amount = Long.valueOf(amountStr);

            DataProcessedDay oldData = dataProcessedDayService.get(id);

            if (oldData != null) {
                if ("totalNum".equals(type)) {
                    oldData.setTotalNum(amount);
                } else {
                    oldData.setEventNum(amount);
                }

                oldData.setUpdateTime(new Date());
                dataProcessedDayService.update(oldData);
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
                dataProcessedDayService.save(model);
                finishedMap.put(model.getSerialNumber(), 1);
            }
        }

        SimpleDateFormat format2 = new SimpleDateFormat("yyyyMMdd");

        for (Map.Entry<Integer, Integer> entry : finishedMap.entrySet()) {
            int sn = entry.getKey();
            int status = entry.getValue();

            if (status == 0) {
                Date dayTime = null;
                try {
                    dayTime = format2.parse(String.valueOf(sn));
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

                DataProcessedDay oldData = dataProcessedDayService.get(id);

                if (oldData == null) {
                    model.setTotalNum(0L);
                    model.setEventNum(0L);
                    model.setUpdateTime(new Date());
                    dataProcessedDayService.save(model);
                } else {
                    if ("totalNum".equals(type)) {
                        oldData.setTotalNum(0L);
                    } else {
                        oldData.setEventNum(0L);
                    }
                    oldData.setUpdateTime(new Date());
                    dataProcessedDayService.update(oldData);
                }
            }
        }

        return CommonResponse.getSuccessResponse(new ExcelImportResult(i, errors));
    }


    public static class ImportRequest {
        private String unitId;
        private String eventId;
        private String type;
        private Date startTime;
        private Date endTime;
        private MultipartFile importFile;

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

        public MultipartFile getImportFile() {
            return importFile;
        }

        public void setImportFile(MultipartFile importFile) {
            this.importFile = importFile;
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
    }


    public static class ExcelModel {
        @ReadProperty(cellIndex = 0)
        private String day;

        @ReadProperty(cellIndex = 1)
        private String amount;

        public String getAmount() {
            return amount;
        }

        public void setAmount(String amount) {
            this.amount = amount;
        }

        public String getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = day;
        }
    }

}
