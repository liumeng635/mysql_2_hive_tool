package com.sync.liumeng.transData.sqoop;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import com.sync.liumeng.exception.ExceptionHandle;
import com.sync.liumeng.mail.SendMail;
import com.sync.liumeng.tool.RemoteShellTool;
import com.sync.liumeng.transData.SyncCommUtil;
import com.sync.liumeng.transData.handler.SyncHandler;
import com.sync.liumeng.transData.handler.UpdateFactory;
import com.sync.liumeng.transData.jdbc.HiveJdbc;
import com.sync.liumeng.transData.jdbc.MysqlJdbc;
import com.sync.liumeng.transData.jdbc.SyncInfoJdbc;

public class SqoopMysql2HiveUpdate extends SyncHandler implements UpdateFactory{
	private static Logger log = Logger.getLogger(SqoopMysql2HiveUpdate.class);
	
	public static String SEPARATOR = "/";
	
	private SqoopMysql2HiveUpdate(){}
	
	public static SqoopMysql2HiveUpdate newInstance(){
		return new SqoopMysql2HiveUpdate();
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
	public static List<Map<String,Object>> generateSqoopCmd(String schema,String tableName,Map<String,Object> syncInfo,String now,String[] partions) throws Exception{
		List<Map<String,Object>> rsList = new ArrayList<>();
		MysqlJdbc dbUtil = MysqlJdbc.getInstance();
		Map<String, List<Map<String,Object>>> rsMap = dbUtil.descTableStruct(tableName);
		try {
			Map<String,Object> tMap = null;
			String partition = "day="+partions[0]+"/hour="+partions[1];
			String colName = (String)syncInfo.get("basisc_col_name");
			String fetchTime = (String)syncInfo.get("last_fetch_time");
			String condition = colName + ">" + "'"+fetchTime+"' and "+colName+"<="+"'"+now+"' and create_time <> update_time";
			for(String key : rsMap.keySet()) {
				tMap = new HashMap<>();
				tableName = key;//表名
				tMap.put("tableName", tableName);
				tMap.put("jobPath", SyncCommUtil.packageSqoopCmd(schema, tableName, condition, partition));
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
	public void syncMysqlUpdateData2Hive(String schema,String tabName,Map<String,Object> syncInfo) throws Exception{
		RemoteShellTool tool = RemoteShellTool.getInstance();
		HiveJdbc jdbc = HiveJdbc.getInstance();
		String[] partition = SyncCommUtil.getNowDayAndHour();
		//生成的文件拷贝到linux服务器上
		List<Map<String,Object>> syncCmdList =  null;
		String nowTime = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
		//增量同步
		syncCmdList = SqoopMysql2HiveUpdate.generateSqoopCmd(schema,tabName,syncInfo,nowTime,partition);
		SyncInfoJdbc hiveSync = SyncInfoJdbc.getInstance();
		String tableName = null;//主表
		String cmd = null;
		for(Map<String, Object> item : syncCmdList){
			tableName = SyncCommUtil.trimTou((String)item.get("tableName"));
			cmd = (String)item.get("cmd");
			hiveSync.saveOrUpdateUpdateSyncInfo("update_time", nowTime, tableName, schema);
			log.info("正在同步更新数据："+tableName+"到hive上==================");
			log.info(cmd);
			tool.exec(cmd);
			jdbc.excuteAddPartion(schema, tableName, partition);//创建分区
			log.info("同步更新数据到到hive上结束==================");
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
		SqoopMysql2HiveUpdate util = new SqoopMysql2HiveUpdate();
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
