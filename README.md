# mysql_2_hive_tool
# 简介：
  - 本工具功能是将mysql数据同步到hive上，支持datax和sqoop。使用spring-boot框架编写，同步库中所有表，可以部署在不局限hadoop服务器上的任何一台可与之通   讯的机器。
  - 支持增量同步和更新同步，
    - 增量同步机制：根据自增主键增量同步，如果主键非自增主键，则每次先删除hive对应表记录，然后全量同步。
    - 更新数据同步：表中含有update_time字段的才会去更新同步。
  - 使用datax需要远程hive服务器安装datax
  - 使用sqoop需要远程hive服务器安装sqoop
# 使用：
- 如果是开发环境，修改application-dev.yml配置为自己的环境
- 如果是生产环境，修改application-prod.yml配置为生产环境
- 需使用maven环境编译 进入项目根目录 mven clean package上传jar文件到服务器，java -jar启动即可
```
    quartz:
    scheduler: 
        cron: 
          ###增量同步定时器
          increase: 0 0 0/1 * * ?
          ###更新同步定时器
          update: 0 0 0/1 * * ?
    config: 
    #######同步工具(可选datax和sqoop)
    sync_tool: datax
    #######安装hive的linux服务器信息
    remote_ssh_ip: 127.0.0.1
    remote_ssh_user: root
    remote_ssh_pwd: xcs@!#$DSFDF125
    #######datax JSON文件本地路径
    datax_local_path: D:/datax_config ###程序生成的datax json临时文件（如果使用sqoop此项可不配置）
    datax_job_dir: /root/datax/job ###远程hive服务器上存储datax执行同步的json配置文件的位置（如果使用sqoop此项可不配置）
    
    #######linux服务器上datax_py路径
    datax_remote_path: /root/datax/bin/datax.py
    
    ######hadoop文件系统路径
    hadoop_dfs_path: /user/hive/warehouse
    
    #######datax的json文件模板(部署到linux环境时需要将文件拷贝至相关路劲并修改此参数为当前路劲)
    datax_json_template: D:/datax_cfg_template.json
    
    #########hive连接配置
    hive_url: jdbc:hive2://127.0.0.1:10000/yinian
    hive_username: root
    hive_password: root
    hive_driver: org.apache.hive.jdbc.HiveDriver
    
    
    #########hive同步信息库连接配置
    hive_mysql_sync_url: jdbc:mysql://127.0.0.1:3306/hive_sync
    hive_mysql_sync_username: root
    hive_mysql_sync_password: root
    hive_mysql_sync_driver: com.mysql.jdbc.Driver
    
    #####业务库信息
    bi_mysql_url_part: jdbc:mysql://127.0.0.1:3306/
    bi_mysql_username: biuser
    bi_mysql_password: biuser
    bi_mysql_driver: com.mysql.jdbc.Driver
    bi_mysql_schema: yinian
    
    ######mysql内网地址
    bi_mysql_url_inner_ip: 127.0.0.1
    
    ######邮件信息
    from_mail_account: ******@qq.com
    from_mail_pwd: *******
    from_mail_smtp_host: smtp.qq.com
    to_mail_account: liumeng@test.com
    from_personal: 大数据同步平台
    to_personal: 尊敬的工程师
```
