package com.sync.liumeng.config;

import java.sql.SQLException;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import com.sync.liumeng.transData.jdbc.HiveJdbc;
import com.sync.liumeng.transData.jdbc.SyncInfoJdbc;

/** 
 * @ClassName: ApplicationEventListener 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年8月21日 下午4:11:03
 */
@Configuration
public class ApplicationEventListener implements ApplicationListener<ContextRefreshedEvent>{
	
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		try {
			//检测同步信息记录表是否存在
			SyncInfoJdbc.getInstance().checkSyncTableExists();
			//检测对应的hive数据库是否是存在的，不存在新建
			HiveJdbc.getInstance().checkShemaExsists();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
