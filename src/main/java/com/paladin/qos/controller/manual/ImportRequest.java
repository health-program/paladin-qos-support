package com.paladin.qos.controller.manual;

import com.paladin.qos.service.data.dto.ReadHospitalDataEvent;
import org.springframework.web.multipart.MultipartFile;

/**
 * @author TontoZhou
 * @since 2019/12/3
 */
public class ImportRequest extends ReadHospitalDataEvent {

    private MultipartFile importFile;

    public MultipartFile getImportFile() {
        return importFile;
    }

    public void setImportFile(MultipartFile importFile) {
        this.importFile = importFile;
    }

}
