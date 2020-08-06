 #### 提交方式

```shell
rm meta_mgr_java.zip
zip -r meta_mgr_java.zip src pom.xml
# 本地参数
-server -Xms4096m -Xmx4096m -XX:MaxDirectMemorySize=256m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking
```

#### TODO

1. 写时更新旧的version 
2. dio 读取
3. 累加最大的v 用1g内存
4. 增加分区
5. 新生代比例
