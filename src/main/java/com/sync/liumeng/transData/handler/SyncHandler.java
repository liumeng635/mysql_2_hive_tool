package com.sync.liumeng.transData.handler;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import com.sync.liumeng.exception.ExceptionHandle;
import com.sync.liumeng.mail.SendMail;
import com.sync.liumeng.tool.GenerateHiveView;
import com.sync.liumeng.transData.SyncCommUtil;
import com.sync.liumeng.transData.datax.DataxMysql2HiveIncrease;
import com.sync.liumeng.transData.jdbc.HiveJdbc;
import com.sync.liumeng.transData.jdbc.MysqlJdbc;
import com.sync.liumeng.transData.jdbc.SyncInfoJdbc;

/** 
 * @ClassName: SyncIncreaseHandler 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年8月22日 下午2:00:02
 */
public abstract class SyncHandler {
	public static void generateCreateTableSqlFiles(String schema,String path) throws Exception {
		MysqlJdbc dbUtil =  MysqlJdbc.getInstance();
		StringBuilder sb = new StringBuilder();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sb.append(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema)+"\n\n");
		}
		FileUtils.writeStringToFile(new File(path), sb.toString(), "UTF-8");
	}
	
	/**
	 * 所有表的hive创建sql
	 * @Title: generateCreateTableSqlList 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<String>    返回类型 
	 * @throws
	 */
	public static List<Map<String,String>> generateCreateTableSqlList(String schema) throws Exception {
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<Map<String,String>> rs = new ArrayList<Map<String,String>>();
		Map<String,String> sqlmap = null;
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			sqlmap = new HashMap<String,String>();
			sqlmap.put("sql", SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
			sqlmap.put("table", tableName);
			rs.add(sqlmap);
			/*if(dbUtil.isContainUpdate(SyncCommUtil.trimTou(tableName))){
				rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(SyncCommUtil.trimTou(tableName)+"_update", listCol, schema));//更新从表  约定记录更新记录的从表后缀在主表基础上加上_update
			}*/
		}
		return rs;
	}
	
	/**
	 * 单表hive创建sql
	 * @Title: generateCreateTableSql 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param table
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<String>    返回类型 
	 * @throws
	 */
	public static List<String> generateCreateTableSql(String schema,String table) throws Exception {
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(table);
		String tableName = "";
		List<Map<String,Object>> listCol = null;
		List<String> rs = new ArrayList<>();
		for(String key : rsMap.keySet()) {
			tableName = key;//表名
			listCol = rsMap.get(key);//表字段信息
			rs.add(SyncCommUtil.gerateCreateTableStoreAsTextSql(tableName, listCol, schema));
		}
		return rs;
	}
	
	/**
	 * 生成mysql对应字段和hive字段信息
	 * @Title: generateHiveColInfo 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param cols
	 * @param @return    设定文件 
	 * @return String    返回类型 
	 * @throws
	 */
	public static Map<String,String> generateHiveColInfo(List<Map<String, Object>> cols){
		StringBuilder sb1 = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		Map<String,String> rsMap = new HashMap<>();
		for(Map<String, Object> map : cols){
			sb1.append("{\"name\":\""+map.get("code")+"\",\"type\":\""+map.get("valueType")+"\"},");
			sb2.append("\""+map.get("code")+"\",");
		}
		String rs1 = sb1.toString();
		rs1 = rs1.substring(0,rs1.lastIndexOf(","));//mysql字段信息
		String rs2 = sb2.toString();
		rs2 = rs2.substring(0,rs2.lastIndexOf(","));//hive字段信息
		rsMap.put("mysql_cols", rs2);
		rsMap.put("hive_cols", rs1);
		return rsMap;
	}
	/**
	 * 新表不创建只是记录下同步信息
	 * @param tableName
	 * @param schema
	 * @throws Exception
	 */
	public static void recordNewTablesSync(String tableName,String schema) throws Exception {
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		if(mysql.isContainUpdate(tableName)) {
			hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
		}
		if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
			 String pkName = mysql.getTablePk(tableName);
			 hiveSync.saveOrUpdateMaxRecd(pkName, "0", tableName,schema);//记录下最大值0
		}
	}
	
	/**
	 * 将mysql所有表创建到hive上
	 * @Title: createListAllMysqlTable2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void createListAllMysqlTable2Hive(String schema) throws Exception{
		List<Map<String,String>> list = DataxMysql2HiveIncrease.generateCreateTableSqlList(schema);
		HiveJdbc dbh = HiveJdbc.getInstance();
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		try {
			String sql = null;
			String tableName = null;
			 Map<String,Object> valMap = null;
			for(Map<String,String> sqMap : list){
				sql = sqMap.get("sql");
				tableName = sqMap.get("table");
				dbh.createTable(sql);
				GenerateHiveView.gerateView(schema,tableName);
				if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
					 valMap = mysql.selectPkIdMax(tableName);
					 String pkName = (String)valMap.get("pkName");
					 hiveSync.saveOrUpdateMaxRecd(pkName, "0", tableName,schema);//记录下最大值
				 }
				 if(mysql.isContainUpdate(tableName)) {//含有更新字段
					 hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
				 }
			}
		} catch (Exception e) {
			e.printStackTrace();
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
		}
	}
	
	/**
	 * mysql单表hive创建
	 * @Title: createMysqlTable2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param table
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	public void createMysqlTable2Hive(String schema,String table) throws Exception{
		List<String> list = DataxMysql2HiveIncrease.generateCreateTableSql(schema,table);
		HiveJdbc dbh = HiveJdbc.getInstance();
		try {
			for(String sql : list){
				dbh.createTable(sql);//建表
				GenerateHiveView.gerateView(schema,table);
			}
		} catch (Exception e) {
			e.printStackTrace();
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}finally {
			dbh.releaseConn();
		}
	}
}
