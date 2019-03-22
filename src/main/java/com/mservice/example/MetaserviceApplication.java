package com.mservice.example;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.mservice.example.mapper")
public class MetaserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(MetaserviceApplication.class, args);
	}

}
