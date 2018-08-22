package com.sync.liumeng.factory;

import com.sync.liumeng.config.MonitorConfig;
import com.sync.liumeng.transData.datax.DataxMysql2HiveIncrease;
import com.sync.liumeng.transData.handler.IncreaseFactory;
import com.sync.liumeng.transData.sqoop.SqoopMysql2HiveIncrease;

/** 
 * @ClassName: SyncIncreaseFactory 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年8月22日 下午1:47:11
 */
public class SyncIncreaseFactory {
	
	public static IncreaseFactory createSyncEngine(){
		switch (MonitorConfig.SYNC_TOOL){
			case "datax": return  DataxMysql2HiveIncrease.newInstance();
			case "sqoop":return  SqoopMysql2HiveIncrease.newInstance();
			default:throw new RuntimeException("您配置的同步工具不正确，请在配置文件中配置config.sync_tool为datax或sqoop");
		}
	}
}
