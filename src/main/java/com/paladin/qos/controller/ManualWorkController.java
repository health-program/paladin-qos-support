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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

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

            String amountStr = excelData.getAmount();
            String dayStr = excelData.getDay();

            Date dayTime = null;
            try {
                dayTime = format.parse(dayStr);
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
            model.setSerialNumber(serialNumber);

            amountStr = amountStr.replaceAll("\\.", "");
            amountStr = amountStr.substring(0, Math.min(amountStr.length(), 8));
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
            }
        }

        return CommonResponse.getSuccessResponse(new ExcelImportResult(i, errors));
    }


    public static class ImportRequest {
        private String unitId;
        private String eventId;
        private String type;
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
