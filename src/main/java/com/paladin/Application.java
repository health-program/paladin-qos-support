package com.paladin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.paladin.framework.core.GlobalProperties;

@EnableWebMvc
@EnableTransactionManagement
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
public class Application {

	public static void main(String[] args) {
		GlobalProperties.project = "qos";
		SpringApplication.run(Application.class, args);
	}

}
