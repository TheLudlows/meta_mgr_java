 #### 提交方式

```shell
rm meta_mgr_java.zip
zip -r meta_mgr_java.zip src pom.xml
# 本地参数
-server -Xms4096m -Xmx4096m -XX:MaxDirectMemorySize=256m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking
```

#### 思路想法

1. 写时更新旧的version 
2. dio 读取

