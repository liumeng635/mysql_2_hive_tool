package com.sync.liumeng.factory;

import com.sync.liumeng.config.MonitorConfig;
import com.sync.liumeng.transData.datax.DataxMysql2HiveUpdate;
import com.sync.liumeng.transData.handler.UpdateFactory;
import com.sync.liumeng.transData.sqoop.SqoopMysql2HiveUpdate;

/** 
 * @ClassName: SyncIncreaseFactory 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年8月22日 下午1:47:11
 */
public class SyncUpdateFactory {
	
	public static UpdateFactory createSyncEngine(){
		switch (MonitorConfig.SYNC_TOOL){
			case "datax": return  DataxMysql2HiveUpdate.newInstance();
			case "sqoop":return  SqoopMysql2HiveUpdate.newInstance();
			default:throw new RuntimeException("您配置的同步工具不正确，请在配置文件中配置config.sync_tool为datax或sqoop");
		}
	}
}
