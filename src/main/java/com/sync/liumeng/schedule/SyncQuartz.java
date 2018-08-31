package com.sync.liumeng.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.sync.liumeng.config.MonitorConfig;
import com.sync.liumeng.factory.SyncIncreaseFactory;
import com.sync.liumeng.factory.SyncUpdateFactory;
import com.sync.liumeng.transData.handler.IncreaseFactory;
import com.sync.liumeng.transData.handler.UpdateFactory;

@Component
@Configurable
@EnableScheduling
public class SyncQuartz {
	private static final Logger LOGGER =  LoggerFactory.getLogger(SyncQuartz.class);
	
	private String SCHEMA = MonitorConfig.BI_MYSQL_SCHEMA;
	
	@Async
  	@Scheduled(cron = "${quartz.scheduler.cron.increase}")
    public void work() throws Exception {
  		LOGGER.info("增量同步任务开始");
  		/*IncreaseFactory syncTool = SyncIncreaseFactory.createSyncEngine();
    	try {
    		LOGGER.info("开始执行");
    		syncTool.addSynMysql2Hive(SCHEMA);
			LOGGER.info("执行结束");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("业务数据同步出现故障",e);
		}finally {
//			MysqlJdbc.getInstance().releaseConn();
//			HiveJdbc.getInstance().releaseConn();
//			RemoteShellTool.getInstance().releaseConn();
		}*/
    }
  	
	@Async
	@Scheduled(cron = "${quartz.scheduler.cron.update}")
    public void work1() throws Exception { 
  		LOGGER.info("更新数据同步任务开始");
  		/*UpdateFactory syncTool = SyncUpdateFactory.createSyncEngine();
    	try {
    		LOGGER.info("开始执行");
    		syncTool.addSynUpdateDataMysql2Hive(SCHEMA);
    		LOGGER.info("执行结束");
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("业务数据同步出现故障",e);
		}finally {
//			MysqlJdbc.getInstance().releaseConn();
//			HiveJdbc.getInstance().releaseConn();
//			RemoteShellTool.getInstance().releaseConn();
		}*/
    }
}
