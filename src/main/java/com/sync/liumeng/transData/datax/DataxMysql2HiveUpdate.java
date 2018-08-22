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
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.sync.liumeng.config.MonitorConfig;
import com.sync.liumeng.exception.ExceptionHandle;
import com.sync.liumeng.mail.SendMail;
import com.sync.liumeng.tool.RemoteShellTool;
import com.sync.liumeng.transData.SyncCommUtil;
import com.sync.liumeng.transData.handler.SyncHandler;
import com.sync.liumeng.transData.handler.UpdateFactory;
import com.sync.liumeng.transData.jdbc.HiveJdbc;
import com.sync.liumeng.transData.jdbc.MysqlJdbc;
import com.sync.liumeng.transData.jdbc.SyncInfoJdbc;

@Component
public class DataxMysql2HiveUpdate extends SyncHandler implements UpdateFactory{
	private static Logger log = Logger.getLogger(DataxMysql2HiveUpdate.class);
	
	private static String DATAX_JSON_TEMPLATE = MonitorConfig.DATAX_JSON_TEMPLATE;
	
	private static String DATAX_LOCAL_PATH = MonitorConfig.DATAX_LOCAL_PATH;
	
	private static String DATAX_JOB_DIR = MonitorConfig.DATAX_JOB_DIR;
	
	private DataxMysql2HiveUpdate(){}
	
	public static DataxMysql2HiveUpdate newInstance(){
		return new DataxMysql2HiveUpdate();
	}
	
	public static String SEPARATOR = "/";
	
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
	public static List<Map<String,Object>> generateTableDataxJsonCfg(String dir,String schema,String tableName,Map<String,Object> syncInfo,String now,String[] partions) throws Exception{
		File dieF = FileUtils.getFile(dir);
		if(!dieF.exists()){
			FileUtils.forceMkdir(dieF);
		}
		List<Map<String,Object>> rsList = new ArrayList<>();
		String temStr = FileUtils.readFileToString(new File(DATAX_JSON_TEMPLATE), "UTF-8");
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
			String colName = (String)syncInfo.get("basisc_col_name");
			String fetchTime = (String)syncInfo.get("last_fetch_time");
			String condition = colName + ">" + "'"+fetchTime+"' and "+colName+"<="+"'"+now+"' and create_time <> update_time";
			jdbc.excuteAddPartion(schema, tableName, partions);//创建分区
			
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				listCol = rsMap.get(key);//表字段信息
				colMap = generateHiveColInfo(listCol);
				jsonContent = temStr;
				jsonContent = jsonContent.replaceAll("\\$\\{schema\\}", SyncCommUtil.trimTou(schema));
				jsonContent = jsonContent.replaceAll("\\$\\{table\\}", SyncCommUtil.trimTou(tableName));
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_table\\}", tableName);
				jsonContent = jsonContent.replaceAll("\\$\\{mysql_cols\\}", colMap.get("mysql_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{hive_cols\\}", colMap.get("hive_cols"));
				jsonContent = jsonContent.replaceAll("\\$\\{condition\\}",condition);//按照更新时间同步的
				jsonContent = jsonContent.replaceAll("\\$\\{partion\\}", partition);//分区
				dataxJsonPath = dir+SEPARATOR+SyncCommUtil.trimTou(tableName)+"_update"+System.currentTimeMillis()+".json";
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
	 * 单表增量导入更新数据
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
	public void syncMysqlUpdateData2Hive(String schema,String tabName,Map<String,Object> syncInfo){
		RemoteShellTool tool = RemoteShellTool.getInstance();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> jsonFiles =  null;
		
		String nowTime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
		//增量同步
		try {
			jsonFiles = DataxMysql2HiveUpdate.generateTableDataxJsonCfg(DATAX_JSON_TEMPLATE, schema,tabName,syncInfo,nowTime,SyncCommUtil.getNowDayAndHour());
			if(!new File(DATAX_JSON_TEMPLATE).exists()){
				FileUtils.forceMkdir(new File(DATAX_JSON_TEMPLATE));
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
			String tableName = "";//主表
			String jsonP = "";
			for(Map<String, Object> json : jsonFiles){
				try {
					jsonP = dir+SEPARATOR+new File(String.valueOf(json.get("jobPath"))).getName();
					tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
					tool.transFile2Linux(String.valueOf(json.get("jobPath")), dir);
					log.info("正在同步更新数据："+tableName+"到hive上==================");
					tool.excuteDataxTransData(jsonP);
					hiveSync.saveOrUpdateUpdateSyncInfo("update_time", nowTime, tableName, schema);
					log.info("同步更新数据到到hive上结束==================");
				} catch (SQLException e) {
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
	 * 增量同步更新数据到hive
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
	public void addSynUpdateDataMysql2Hive(String schema){
		MysqlJdbc yinan = MysqlJdbc.getInstance();
		//检测mysql上的所有表
		List<Map<String, Object>> allMysqlTables = null;
		try {
			allMysqlTables = MysqlJdbc.getInstance().findAllTables(schema);
		} catch (SQLException e1) {
			log.error(e1);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e1), null);
		}
		Map<String,Object> syncTbInfo = null;
		String tbName = "";
		for(Map<String, Object> tMap : allMysqlTables){
			tbName = (String)tMap.get("TABLE_NAME");
			try {
				if(!yinan.isContainUpdate(tbName) || !yinan.hasPk(tbName)){//不是含更新字段的表 或者是没有主键的表
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			
			//同步数据
			try {
				syncTbInfo = SyncCommUtil.returnHiveTUpdateableSynInfo(tbName,schema);
				if(syncTbInfo == null){
					return;
				}
				this.syncMysqlUpdateData2Hive(schema, tbName,syncTbInfo);
			} catch (Exception e) {
				log.error(e);
				SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		DataxMysql2HiveUpdate util = new DataxMysql2HiveUpdate();
		util.addSynUpdateDataMysql2Hive("yinian");
		
		MysqlJdbc.getInstance().releaseConn();
	}
	
	public static void clear() throws SQLException{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc hive = HiveJdbc.getInstance();
		hive.excuteSql(" drop database yinian cascade");
		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
		hive.excuteSql(" create database yinian");
	}
}
