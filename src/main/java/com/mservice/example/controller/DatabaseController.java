package com.mservice.example.controller;

import com.mservice.example.entity.Database;
import com.mservice.example.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class DatabaseController {
  @Autowired
  private DatabaseService databaseService;

    @GetMapping("/database")
  public List<Database> lists() {
    return databaseService.getDatabases();
  }
}
