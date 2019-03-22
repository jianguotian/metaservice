package com.mservice.example.controller.metastore;

import com.mservice.example.entity.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@RequestMapping(value = "/{dbname}")     // 通过这里配置使下面的映射都在/databases下，可去除
public class TblController {

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

  @RequestMapping(value = "/{dbname}", method = RequestMethod.GET)
  public List<Table> getTables(@PathVariable String dbname) {
    ArrayList<Table> at = new ArrayList<Table>();
    List<String> ls = null;
    try {
      ls = client.get_all_tables("default");
    } catch (TException e) {
      e.printStackTrace();
    }
    for(int i=0;i<ls.size();i++){
      try {
        at.add(client.get_table("default", ls.get(i)));
      } catch (TException e) {
        e.printStackTrace();
      }
    }
    return at;
  }

  @RequestMapping(value = "/{dbname}/{tblname}", method = RequestMethod.GET)
  public Table getTable(@PathVariable String dbname, @PathVariable String tblname) throws TException {
    ArrayList<Table> at = new ArrayList<Table>();
    Table table = client.get_table(dbname, tblname);
    table.getSd().getCols();
    String formats = "yyyy-MM-dd HH:mm:ss";
    Long timestamp = Long.parseLong(table.getParameters().get("transient_lastDdlTime")) * 1000;
    String date = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(timestamp));
    System.out.println(date);
    return table;
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