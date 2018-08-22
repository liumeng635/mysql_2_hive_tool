package com.sync.liumeng.transData.sqoop;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;

import com.sync.liumeng.exception.ExceptionHandle;
import com.sync.liumeng.mail.SendMail;
import com.sync.liumeng.tool.RemoteShellTool;
import com.sync.liumeng.transData.SyncCommUtil;
import com.sync.liumeng.transData.handler.IncreaseFactory;
import com.sync.liumeng.transData.handler.SyncHandler;
import com.sync.liumeng.transData.jdbc.HiveJdbc;
import com.sync.liumeng.transData.jdbc.MysqlJdbc;
import com.sync.liumeng.transData.jdbc.SyncInfoJdbc;

public class SqoopMysql2HiveIncrease extends SyncHandler implements IncreaseFactory{
	private static Logger log = Logger.getLogger(SqoopMysql2HiveIncrease.class);
	
	public static String SEPARATOR = "/";
	
	public static SqoopMysql2HiveIncrease newInstance(){
		return new SqoopMysql2HiveIncrease();
	}
	
	/**
	 * 全量同步sqoop的执行命令（table不是空单表同步）
	 * @param schema
	 * @param partions
	 * @return
	 * @throws Exception
	 */
	public static List<Map<String,Object>> generateSqoopCmd(String schema,String[] partions,String table) throws Exception{
		List<Map<String,Object>> rsList = new ArrayList<>();
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = null;
		if(StringUtils.isNotBlank(table)) {
			rsMap = dbUtil.descTableStruct(table);
		}else {
			rsMap = dbUtil.descTableTableStruct();
		}
		 
		try {
			String tableName = "";
			Map<String,Object> tMap = null;
			//组装分区信息
			String partition = "day="+partions[0]+"/hour="+partions[1];
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				tMap.put("tableName", tableName);
				tMap.put("cmd", SyncCommUtil.packageSqoopCmd(schema, tableName, "1=1", partition));//全量的同步
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
	public static List<Map<String,Object>> generateSqoopCmdIncrease(String schema,String tableName,Map<String,Object> syncTbInfo,String[] partions) throws Exception{
		List<Map<String,Object>> rsList = new ArrayList<>();
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			Map<String,Object> tMap = null;
			String partition = "day="+partions[0]+"/hour="+partions[1];
			String pkName = (String)syncTbInfo.get("pk_name");
			String lastMaxVal = (String)syncTbInfo.get("last_max_val");
			String condition = pkName+">"+lastMaxVal;
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				tMap.put("tableName", tableName);
				tMap.put("cmd", SyncCommUtil.packageSqoopCmd(schema, tableName,condition, partition));
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
		 HiveJdbc jdbc = HiveJdbc.getInstance();
		 String[] partition = SyncCommUtil.getNowDayAndHour();
    	 //生成的文件拷贝到linux服务器上
    	 List<Map<String,Object>> syncCmdlist =  null;
    	 if(all){//所有表全量同步
    		 syncCmdlist = SqoopMysql2HiveIncrease.generateSqoopCmd(schema,partition,null);
    	 }else{//单表全量同步
    		 syncCmdlist = SqoopMysql2HiveIncrease.generateSqoopCmd(schema,partition,tabName);
    	 }
    	
		 MysqlJdbc mysql = MysqlJdbc.getInstance();
		 try {
			SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
			String tableName = null;
			String cmd = null;
			 for(Map<String,Object> item : syncCmdlist){
				 tableName = SyncCommUtil.trimTou((String)item.get("tableName"));
				 cmd = (String)item.get("cmd");
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
				 log.info("正在同步表："+tableName+"的数据到hive上。。。。。。。。。。。。。。。。。。");
				 log.info(cmd);
				 tool.exec(cmd);//执行同步
				 jdbc.excuteAddPartion(schema, tableName, partition);
				 log.info("同步表："+tableName+"的数据到hive上完成。。。。。。。。。。。。。。。。。。");
			 }
		} catch (Exception e) {
			log.error(e);
			SendMail.sendMail("数据同步出错", ExceptionHandle.getErrorInfoFromException(e), null);
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
	public void syncMysqlIncreaseData2Hive(String schema,String tabName,Map<String,Object> syncInfo) throws Exception{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		MysqlJdbc mysql = MysqlJdbc.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		String[] partition = SyncCommUtil.getNowDayAndHour();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> syncCmdlist =  null;
		//判断是否是有主键表 有增量 无全量
		if(mysql.hasPk(tabName)){//如果有 增量
			syncCmdlist = SqoopMysql2HiveIncrease.generateSqoopCmdIncrease(schema,tabName,syncInfo,partition);
		}else{//全量同步
			syncCmdlist = SqoopMysql2HiveIncrease.generateSqoopCmd(schema,partition,tabName);
		}
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		String tableName = null;
		String cmd = null;
		for(Map<String, Object> json : syncCmdlist){
			tableName = SyncCommUtil.trimTou((String)json.get("tableName"));
			cmd = (String)json.get("cmd");
			if(mysql.hasPk(tableName)){
				Map<String,Object> valMap = mysql.selectPkIdMax(tableName);//同步的最大id记录
				String recdIdMax = String.valueOf(valMap.get("val"));
				if(StringUtils.isEmpty(recdIdMax) || StringUtils.equals("null", recdIdMax)){
					recdIdMax = "0";
				}
				String pkName = (String)valMap.get("pkName");
				hiveSync.saveOrUpdateMaxRecd(pkName, recdIdMax, tableName,schema);//记录下最大值
			}else{//无主键  先删除再全量同步
				//将hive上表的数据删除掉
				tool.truncateHiveHdfsRubish(tabName,schema);
				//将hive在hdfs上的垃圾清除掉
				tool.truncateHiveHdfsRubish(tableName,schema);
			}	
			log.info("正在增量同步表："+tableName+"到hive上==================");
			log.info(cmd);
			tool.exec(cmd);
			jdbc.excuteAddPartion(schema, tableName, partition);//创建分区
			log.info("增量同步到hive上结束==================");
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
	
	public static void main(String[] args) throws Exception {
		SqoopMysql2HiveIncrease util = new SqoopMysql2HiveIncrease();
		util.createListAllMysqlTable2Hive("yinian");
//		util.syncMysqlData2Hive("yinian", true, null);
//		util.addSynMysql2Hive("yinian");
//		createSql2File("yinian", "D://createSql.txt");
//		clear();
		MysqlJdbc.getInstance().releaseConn();
	}
	
	public static void clear() throws SQLException{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc hive = HiveJdbc.getInstance();
		hive.excuteSql(" drop database yinian cascade");
//		tool.exec(" hadoop fs -rmr .Trash/Current/user/hive/warehouse/yinian.db");
		tool.exec(" hadoop fs -rmr .Trash/");
		hive.excuteSql(" create database yinian");
	}
}
