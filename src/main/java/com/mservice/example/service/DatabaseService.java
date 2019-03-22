package com.mservice.example.service;

import com.mservice.example.entity.Database;
import com.mservice.example.mapper.DatabaseMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatabaseService {
  @Autowired
  private DatabaseMapper databaseMapper;

  public List<Database> getDatabases() {
    return databaseMapper.getDatabases();
  }
}
