package com.sync.liumeng.transData.handler;

import java.util.Map;

/** 
 * @ClassName: IncreaseFactory 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018年8月22日 下午2:41:58
 */
public interface UpdateFactory {
	public void syncMysqlUpdateData2Hive(String schema,String tabName,Map<String,Object> syncInfo) throws Exception;
	public void addSynUpdateDataMysql2Hive(String schema);
}
