package com.sync.liumeng.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @ClassName: MonitorConfig
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author 刘猛
 * @date 2018年8月21日 下午5:42:05
 */

@Component
public class MonitorConfig {
	public static String BI_MYSQL_SCHEMA;

	public static String FROM_MAIL_ACCOUNT;

	public static String FROM_MAIL_PWD;

	// 发件人邮箱的 SMTP 服务器地址, 必须准确, 不同邮件服务器地址不同, 一般(只是一般, 绝非绝对)格式为: smtp.xxx.com
	// 网易163邮箱的 SMTP 服务器地址为: smtp.163.com
	public static String FROM_MAIL_SMTP_HOST;

	// 收件人邮箱（替换为自己知道的有效邮箱）
	public static String TO_MAIL_ACCOUNT;

	public static String FROM_PERSONAL;

	public static String TO_PERSONAL;

	public static String REMOTE_SSH_IP;

	public static String REMOTE_SSH_USER;

	public static String REMOTE_SSH_PWD;

	public static String DATAX_REMOTE_PATH;

	public static String HADOOP_DFS_PATH;

	// 表示定义数据库的用户名
	public static String HIVE_USERNAME;

	// 定义数据库的密码
	public static String HIVE_PASSWORD;

	// 定义数据库的驱动信息
	public static String HIVE_DRIVER;

	// 定义访问数据库的地址
	public static String HIVE_URL;

	// 表示定义数据库的用户名
	public static String BI_MYSQL_USERNAME;

	// 定义数据库的密码
	public static String BI_MYSQL_PASSWORD;

	// 定义数据库的驱动信息
	public static String BI_MYSQL_DRIVER;

	public static String url_part1;

	public static String url_part2;

	// 表示定义数据库的用户名
	public static String MYSQL_SYNC_USERNAME;

	// 定义数据库的密码
	public static String MYSQL_SYNC_PASSWORD;

	// 定义数据库的驱动信息
	public static String MYSQL_SYNC_DRIVER;

	// 定义访问数据库的地址
	public static String MYSQL_SYNC_URL;

	public static String DATAX_JSON_TEMPLATE;

	public static String DATAX_LOCAL_PATH;

	public static String DATAX_JOB_DIR;
	
	public static String BI_MYSQL_URL_INNER_IP;
	
	public static String SYNC_TOOL;

	@Value("${config.bi_mysql_schema}")
	public void setBI_MYSQL_SCHEMA(String bI_MYSQL_SCHEMA) {
		BI_MYSQL_SCHEMA = bI_MYSQL_SCHEMA;
	}

	@Value("${config.from_mail_account}")
	public void setFROM_MAIL_ACCOUNT(String fROM_MAIL_ACCOUNT) {
		FROM_MAIL_ACCOUNT = fROM_MAIL_ACCOUNT;
	}

	@Value("${config.from_mail_pwd}")
	public void setFROM_MAIL_PWD(String fROM_MAIL_PWD) {
		FROM_MAIL_PWD = fROM_MAIL_PWD;
	}

	@Value("${config.from_mail_smtp_host}")
	public void setFROM_MAIL_SMTP_HOST(String fROM_MAIL_SMTP_HOST) {
		FROM_MAIL_SMTP_HOST = fROM_MAIL_SMTP_HOST;
	}

	@Value("${config.to_mail_account}")
	public void setTO_MAIL_ACCOUNT(String tO_MAIL_ACCOUNT) {
		TO_MAIL_ACCOUNT = tO_MAIL_ACCOUNT;
	}

	@Value("${config.from_personal}")
	public void setFROM_PERSONAL(String fROM_PERSONAL) {
		FROM_PERSONAL = fROM_PERSONAL;
	}

	@Value("${config.to_personal}")
	public void setTO_PERSONAL(String tO_PERSONAL) {
		TO_PERSONAL = tO_PERSONAL;
	}

	@Value("${config.remote_ssh_ip}")
	public void setREMOTE_SSH_IP(String rEMOTE_SSH_IP) {
		REMOTE_SSH_IP = rEMOTE_SSH_IP;
	}

	@Value("${config.remote_ssh_user}")
	public void setREMOTE_SSH_USER(String rEMOTE_SSH_USER) {
		REMOTE_SSH_USER = rEMOTE_SSH_USER;
	}

	@Value("${config.remote_ssh_pwd}")
	public void setREMOTE_SSH_PWD(String rEMOTE_SSH_PWD) {
		REMOTE_SSH_PWD = rEMOTE_SSH_PWD;
	}

	@Value("${config.datax_remote_path}")
	public void setDATAX_REMOTE_PATH(String dATAX_REMOTE_PATH) {
		DATAX_REMOTE_PATH = dATAX_REMOTE_PATH;
	}

	@Value("${config.hadoop_dfs_path}")
	public void setHADOOP_DFS_PATH(String hADOOP_DFS_PATH) {
		HADOOP_DFS_PATH = hADOOP_DFS_PATH;
	}

	@Value("${config.hive_username}")
	public void setHIVE_USERNAME(String hIVE_USERNAME) {
		HIVE_USERNAME = hIVE_USERNAME;
	}

	@Value("${config.hive_password}")
	public void setHIVE_PASSWORD(String hIVE_PASSWORD) {
		HIVE_PASSWORD = hIVE_PASSWORD;
	}

	@Value("${config.hive_driver}")
	public void setHIVE_DRIVER(String hIVE_DRIVER) {
		HIVE_DRIVER = hIVE_DRIVER;
	}

	@Value("${config.hive_url}")
	public void setHIVE_URL(String hIVE_URL) {
		HIVE_URL = hIVE_URL;
	}

	@Value("${config.bi_mysql_username}")
	public void setBI_MYSQL_USERNAME(String bI_MYSQL_USERNAME) {
		BI_MYSQL_USERNAME = bI_MYSQL_USERNAME;
	}

	@Value("${config.bi_mysql_password}")
	public void setBI_MYSQL_PASSWORD(String bI_MYSQL_PASSWORD) {
		BI_MYSQL_PASSWORD = bI_MYSQL_PASSWORD;
	}

	@Value("${config.bi_mysql_driver}")
	public void setBI_MYSQL_DRIVER(String bI_MYSQL_DRIVER) {
		BI_MYSQL_DRIVER = bI_MYSQL_DRIVER;
	}

	@Value("${config.bi_mysql_url_part}")
	public void setUrl_part1(String url_part1) {
		MonitorConfig.url_part1 = url_part1;
	}

	@Value("${config.bi_mysql_schema}")
	public void setUrl_part2(String url_part2) {
		MonitorConfig.url_part2 = url_part2;
	}

	@Value("${config.hive_mysql_sync_username}")
	public void setMYSQL_SYNC_USERNAME(String mYSQL_SYNC_USERNAME) {
		MYSQL_SYNC_USERNAME = mYSQL_SYNC_USERNAME;
	}

	@Value("${config.hive_mysql_sync_password}")
	public void setMYSQL_SYNC_PASSWORD(String mYSQL_SYNC_PASSWORD) {
		MYSQL_SYNC_PASSWORD = mYSQL_SYNC_PASSWORD;
	}

	@Value("${config.hive_mysql_sync_driver}")
	public void setMYSQL_SYNC_DRIVER(String mYSQL_SYNC_DRIVER) {
		MYSQL_SYNC_DRIVER = mYSQL_SYNC_DRIVER;
	}

	@Value("${config.hive_mysql_sync_url}")
	public void setMYSQL_SYNC_URL(String mYSQL_SYNC_URL) {
		MYSQL_SYNC_URL = mYSQL_SYNC_URL;
	}

	@Value("${config.datax_json_template}")
	public void setDATAX_JSON_TEMPLATE(String dATAX_JSON_TEMPLATE) {
		DATAX_JSON_TEMPLATE = dATAX_JSON_TEMPLATE;
	}

	@Value("${config.datax_local_path}")
	public void setDATAX_LOCAL_PATH(String dATAX_LOCAL_PATH) {
		DATAX_LOCAL_PATH = dATAX_LOCAL_PATH;
	}

	@Value("${config.datax_job_dir}")
	public void setDATAX_JOB_DIR(String dATAX_JOB_DIR) {
		DATAX_JOB_DIR = dATAX_JOB_DIR;
	}
	
	@Value("${config.bi_mysql_url_inner_ip}")
	public void setBI_MYSQL_URL_INNER_IP(String bI_MYSQL_URL_INNER_IP) {
		BI_MYSQL_URL_INNER_IP = bI_MYSQL_URL_INNER_IP;
	}

	@Value("${config.sync_tool}")
	public void setSYNC_TOOL(String sYNC_TOOL) {
		SYNC_TOOL = sYNC_TOOL;
	}
	
	
	
	

}
