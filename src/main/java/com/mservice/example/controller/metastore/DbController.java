package com.mservice.example.controller.metastore;

import com.mservice.example.controller.metastore.MetastoreClient;
import com.mservice.example.entity.Database;

import java.util.*;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/databases")     // 通过这里配置使下面的映射都在/databases下，可去除
public class DbController {

  static Map<String, Database> databases = Collections.synchronizedMap(new HashMap<String, Database>());
  static ThriftHiveMetastore.Iface client;

  static {
    try {
      client = new MetastoreClient().open();
    } catch (MetaException e) {
      e.printStackTrace();
    }
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public List<String> getDatabaseList() throws TException {
    // 处理"/databases/"的GET请求，用来获取用户列表
    // 还可以通过@RequestParam从页面中传递参数来进行查询条件或者翻页信息的传递

//    List<Database> r = new ArrayList<Database>(databases.values());
    List<String> r = client.get_all_databases();
    return r;
  }

  @RequestMapping(value = "/", method = RequestMethod.POST)
  public String postDatabase(@ModelAttribute Database database) {
    // 处理"/databases/"的POST请求，用来创建Database
    // 除了@ModelAttribute绑定参数之外，还可以通过@RequestParam从页面中传递参数
    databases.put(database.getName(), database);
    return "success";
  }

  @RequestMapping(value = "/{name}", method = RequestMethod.GET)
  public org.apache.hadoop.hive.metastore.api.Database getDatabase(@PathVariable String name) throws TException {
    // 处理"/databases/{name}"的GET请求，用来获取url中name值的Database信息
    // url中的name可通过@PathVariable绑定到函数的参数中
    return client.get_database(name);
//    return databases.get(name);
  }

  @RequestMapping(value = "/{name}", method = RequestMethod.PUT)
  public String putDatabase(@PathVariable String name, @ModelAttribute Database database) {
    // 处理"/databases/{name}"的PUT请求，用来更新Database信息
    Database u = databases.get(name);
    u.setName(database.getName());
    u.setDescription(database.getDescription());
    databases.put(name, u);
    return "success";
  }

  @RequestMapping(value = "/{name}", method = RequestMethod.DELETE)
  public String deleteDatabase(@PathVariable String name) {
    // 处理"/databases/{name}"的DELETE请求，用来删除Database
    databases.remove(name);
    return "success";
  }

}