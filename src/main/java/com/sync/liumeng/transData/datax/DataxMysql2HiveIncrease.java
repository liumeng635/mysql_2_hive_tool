package com.sync.liumeng.transData.datax;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import com.sync.liumeng.config.MonitorConfig;
import com.sync.liumeng.exception.ExceptionHandle;
import com.sync.liumeng.mail.SendMail;
import com.sync.liumeng.tool.RemoteShellTool;
import com.sync.liumeng.transData.SyncCommUtil;
import com.sync.liumeng.transData.handler.IncreaseFactory;
import com.sync.liumeng.transData.handler.SyncHandler;
import com.sync.liumeng.transData.jdbc.HiveJdbc;
import com.sync.liumeng.transData.jdbc.MysqlJdbc;
import com.sync.liumeng.transData.jdbc.SyncInfoJdbc;
import com.sync.liumeng.util.ReadConfUtil;

@Component
public class DataxMysql2HiveIncrease extends SyncHandler implements IncreaseFactory{
	private static Logger log = Logger.getLogger(DataxMysql2HiveIncrease.class);
	
	private static String DATAX_JSON_TEMPLATE = MonitorConfig.DATAX_JSON_TEMPLATE;
	
	private static String DATAX_LOCAL_PATH = MonitorConfig.DATAX_LOCAL_PATH;
	
	private static String DATAX_JOB_DIR = MonitorConfig.DATAX_JOB_DIR;
	
	private DataxMysql2HiveIncrease(){};
	
	public static DataxMysql2HiveIncrease newInstance(){
		return new DataxMysql2HiveIncrease();
	}
	
	public static String SEPARATOR = "/";
	
	/**
	 * 全部表创建datax json配置文件
	 * @Title: generateDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateDataxJsonCfg(String dir,String schema,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String temStr = ReadConfUtil.loadConf2String(DATAX_JSON_TEMPLATE);
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableTableStruct();
		try {
			String tableName = "";
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			
			//组装分区信息
			String partition = "day="+partions[0]+"/hour="+partions[1];
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}",tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}", "");//全量同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", tableName);
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		return rsList;
	}
	
	/**
	 * 单表创建datax json配置文件
	 * @Title: generateTableDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateTableDataxJsonCfg(String dir,String schema,String tableName,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String temStr = ReadConfUtil.loadConf2String(DATAX_JSON_TEMPLATE);
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			HiveJdbc jdbc = HiveJdbc.getInstance();
			String partition = "day="+partions[0]+"/hour="+partions[1];
			Map<String,Object> tMap = null;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}", "");//全量同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", tableName);
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		return rsList;
	}
	
	/**
	 * 单表增量同步
	 * @Title: generateTableIncreaseDataxJsonCfg 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param dir
	 * @param @param schema
	 * @param @param tableName
	 * @param @param syncTbInfo
	 * @param @return
	 * @param @throws Exception    设定文件 
	 * @return List<Map<String,Object>>    返回类型 
	 * @throws
	 */
	public static List<Map<String,Object>> generateTableIncreaseDataxJsonCfg(String dir,String schema,String tableName,Map<String,Object> syncTbInfo,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String temStr = ReadConfUtil.loadConf2String(DATAX_JSON_TEMPLATE);
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			List<Map<String,Object>> listCol = null;
			Map<String,String> colMap = null;
			String jsonContent = "";//datax json配置内容
			String dataxJsonPath = "";
			Map<String,Object> tMap = null;
			String partition = "day="+partions[0]+"/hour="+partions[1];
			String pkName = (String)syncTbInfo.get("pk_name");
			String lastMaxVal = (String)syncTbInfo.get("last_max_val");
			String condition = pkName+">"+lastMaxVal;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}",condition);//全量同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+System.currentTimeMillis()+".json";
				//生成JSON配置文件
				File file = FileUtils.getFile(dataxJsonPath);
				FileUtils.writeStringToFile(file, jsonContent, "UTF-8");
				tMap.put("tableName", tableName);
				tMap.put("jobPath", dataxJsonPath);
				rsList.add(tMap);
			}
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		return rsList;
	}
	
	
	
	/**
	 * 将mysql表全量同步到hive上
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param hiveIpAddr
	 * @param @param hiveUser
	 * @param @param hivePwd
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	@Override
	public void syncMysqlData2Hive(String schema,boolean all,String tabName) throws Exception{
		 RemoteShellTool tool = RemoteShellTool.getInstance();
    	 //生成的文件拷贝到linux服务器上
    	 List<Map<String,Object>> jsonFiles =  null;
    	 if(all){
    		 jsonFiles = DataxMysql2HiveIncrease.generateDataxJsonCfg(DATAX_LOCAL_PATH, schema,SyncCommUtil.getNowDayAndHour());
    	 }else{
    		 jsonFiles = DataxMysql2HiveIncrease.generateTableDataxJsonCfg(DATAX_LOCAL_PATH, schema,tabName,SyncCommUtil.getNowDayAndHour());
    	 }
    	 if(!new File(DATAX_LOCAL_PATH).exists()){
    		 FileUtils.forceMkdir(new File(DATAX_LOCAL_PATH));
    	 }
    	 String now = System.currentTimeMillis()+"";
    	 String dir = DATAX_JOB_DIR+SEPARATOR+now;
    	 boolean exec = tool.exec("mkdir "+dir);
    	 if(exec){
    		 MysqlJdbc mysql = MysqlJdbc.getInstance();
    		 try {
				SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
				 for(Map<String,Object> json : jsonFiles){
					 String jsonP = dir+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
					 String tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					 tool.transFile2Linux(String.valueOf(json.get("jobPath")), dir);
					 log.info("正在同步表："+tableName+"的数据到hive上。。。。。。。。。。。。。。。。。。");
					 tool.excuteDataxTransData(jsonP);
					 if(mysql.hasPk(tableName)){//如果是存在主键的话记录信息
						 Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
						 String recdIdMax = String.valueOf(valMap.get("val"));
						 if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
								recdIdMax = "0";
						 }
						 String pkName = (String)valMap.get("pkName");
						 hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
					 }
					 if(mysql.isContainUpdate(tableName)) {//含有更新字段
						 hiveSync.saveOrUpdateUpdateSyncInfo("update_time", DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"), tableName, schema);//创建更新从表
					 }
					 log.info("同步表："+tableName+"的数据到hive上完成。。。。。。。。。。。。。。。。。。");
				 }
				//最后删除掉中间的过程文件
				 tool.exec("rm -rf "+dir);
				 FileUtils.cleanDirectory(new File(DATAX_LOCAL_PATH));
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
    	 }
	}
	
	/**
	 * 单表增量导数
	 * @Title: syncMysqlData2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param schema
	 * @param @param tabName
	 * @param @param syncInfo
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	@Override
	public void syncMysqlIncreaseData2Hive(String schema,String tabName,Map<String,Object> syncInfo){
		RemoteShellTool tool = RemoteShellTool.getInstance();
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> jsonFiles =  null;
		//判断是否是有主键表 有增量 无全量
		try {
			if(mysql.hasPk(tabName)){//如果有 增量
				jsonFiles = DataxMysql2HiveIncrease.generateTableIncreaseDataxJsonCfg(DATAX_LOCAL_PATH, schema,tabName,syncInfo,SyncCommUtil.getNowDayAndHour());
			}else{//全量同步
				jsonFiles = DataxMysql2HiveIncrease.generateTableDataxJsonCfg(DATAX_LOCAL_PATH, schema,tabName,SyncCommUtil.getNowDayAndHour());
			}
			if(!new File(DATAX_LOCAL_PATH).exists()){
				FileUtils.forceMkdir(new File(DATAX_LOCAL_PATH));
			}
		} catch (IOException e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
		}
		String now = System.currentTimeMillis()+"";
		String dir = DATAX_JOB_DIR+SEPARATOR+now;
		boolean exec = tool.exec("mkdir "+dir);
		if(exec){
			SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
			for(Map<String, Object> json : jsonFiles){
				try {
					String jsonP = dir+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
					String tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					tool.transFile2Linux(String.valueOf(json.get("jobPath")), dir);
					if(mysql.hasPk(tableName)){
						Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
						String recdIdMax = String.valueOf(valMap.get("val"));
						if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
							recdIdMax = "0";
						}
						String pkName = (String)valMap.get("pkName");
						log.info("正在增量同步表："+tableName+"到hive上==================");
						tool.excuteDataxTransData(jsonP);
						log.info("增量同步到hive上结束==================");
						hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
					}else{//无主键  先删除再全量同步
						//将hive上表的数据删除掉
						tool.truncateHiveTableData(tabName,schema,SyncCommUtil.getPreDayAndHour());
						//将hive在hdfs上的垃圾清除掉
						tool.truncateHiveHdfsRubish(tableName,schema);
						//删掉hive表的上个分区数据
						HiveJdbc.getInstance().dropTablePartiton(schema, tabName, SyncCommUtil.getPreDayAndHour());
						log.info("正在增量同步表："+tableName+"到hive上==================");
						tool.excuteDataxTransData(jsonP);
						log.info("增量同步到hive上结束==================");
					}
				} catch (SQLException e) {
					log.error(e);
					SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
				} catch (Exception e) {
					log.error(e);
					SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
				}
			}
			 //最后删除掉中间的过程文件
			 tool.exec("rm -rf "+dir);
			 try {
				FileUtils.cleanDirectory(new File(DATAX_LOCAL_PATH));
			} catch (IOException e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
	
	/**
	 * 增量同步到hive
	 * @Title: addSynMysql2Hive 
	 * @Description: TODO(这里用一句话描述这个方法的作用) 
	 * @param @param hiveIpAddr
	 * @param @param hiveUser
	 * @param @param hivePwd
	 * @param @param schema
	 * @param @throws Exception    设定文件 
	 * @return void    返回类型 
	 * @throws
	 */
	@Override
	public void addSynMysql2Hive(String schema){
		HiveJdbc hive = HiveJdbc.getInstance();
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = null;
		try {
			allMysqlTables = MysqlJdbc.getInstance().findAllTables(schema);
		} catch (SQLException e1) {
			log.error(e1);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e1), null);
		}
		Map<String,Object> syncTbInfo = null;
		for(Map<String, Object> tMap : allMysqlTables){
			try {
				String tbName = (String)tMap.get("TABLE_NAME");
				if(hive.checkTableExists(tbName,schema)){//如果存在
					syncTbInfo = SyncCommUtil.returnHiveTableSynInfo(tbName);
					//增量同步（有主键的增量去同步，无主键的先删除记录然后全量同步）
					this.syncMysqlIncreaseData2Hive(schema, tbName, syncTbInfo);
				}else{//如果不存在
					this.createMysqlTable2Hive(schema,tbName);//创建hive表
					this.syncMysqlData2Hive(schema,false,tbName);//全量同步
					recordNewTablesSync(tbName, schema);//记录同步信息
				}
			} catch (SQLException e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
//	public static void main(String[] args) throws Exception {
//		Mysql2HiveUtil util = new Mysql2HiveUtil();
//		SyncInfoJdbc jdbc = SyncInfoJdbc.getInstance();
//		try {
//			clear();
//			jdbc.truncateTables();
//			util.createListAllMysqlTable2Hive("yinian");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		MysqlJdbc.getInstance().releaseConn();
//	}
//	
//	public static void clear() throws SQLException{
//		RemoteShellTool tool = RemoteShellTool.getInstance();
//		HiveJdbc hive = HiveJdbc.getInstance();
//		hive.excuteSql(" drop database yinian cascade");
//		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
//		hive.excuteSql(" create database yinian");
//	}
}
