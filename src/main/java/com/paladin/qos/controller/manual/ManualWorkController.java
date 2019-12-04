package com.paladin.qos.controller.manual;

import com.paladin.data.dynamic.SqlSessionContainer;
import com.paladin.framework.common.ExcelImportResult;
import com.paladin.framework.core.exception.BusinessException;
import com.paladin.framework.excel.DefaultSheet;
import com.paladin.framework.excel.read.*;
import com.paladin.framework.web.response.CommonResponse;
import com.paladin.qos.service.data.DataProcessedDayService;
import com.paladin.qos.service.data.dto.ReadHospitalData;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author TontoZhou
 * @since 2019/12/3
 */
@Controller
@RequestMapping("/qos/manual")
public class ManualWorkController {

    @Autowired
    private DataProcessedDayService dataProcessedDayService;

    @Autowired
    protected SqlSessionContainer sqlSessionContainer;

    private static final List<ReadColumn> dataImportColumns = DefaultReadColumn.createReadColumn(ExcelModel.class, null);

    @PostMapping("/data/import")
    @ResponseBody
    public Object importData(ImportRequest importRequest) {
        XSSFWorkbook workbook;
        try {
            workbook = new XSSFWorkbook(importRequest.getImportFile().getInputStream());
        } catch (IOException e1) {
            throw new BusinessException("导入异常");
        }

        List<ReadHospitalData> datas = new ArrayList<>();

        ExcelReader<ExcelModel> reader = new ExcelReader<>(ExcelModel.class, dataImportColumns, new DefaultSheet(workbook.getSheetAt(0)), 0);
        List<ExcelImportResult.ExcelImportError> errors = new ArrayList<>();

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");

        int i = 0;

        while (reader.hasNext()) {
            i++;
            if (i > 2000) {
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

            ReadHospitalData data = new ReadHospitalData();
            data.setAmount(Long.valueOf(amountStr));
            data.setDayTime(dayTime);

            datas.add(data);
        }

        importRequest.setDatas(datas);
        dataProcessedDayService.saveDataByManual(importRequest);

        return CommonResponse.getSuccessResponse(new ExcelImportResult(i, errors));
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


    @PostMapping("/data/read")
    @ResponseBody
    public Object readHospitalDataByGroup(ReadRequest readRequest) {
        dataProcessedDayService.readHospitalData(readRequest, readRequest.getSql());
        return CommonResponse.getSuccessResponse();
    }

    @GetMapping("/data/read/status")
    @ResponseBody
    public Object readHospitalDataByGroup() {
        return CommonResponse.getSuccessResponse(dataProcessedDayService.getReadThreadStatus());
    }


}
