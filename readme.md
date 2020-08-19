 #### 提交方式

```shell
rm meta_mgr_java.zip
zip -r meta_mgr_java.zip src pom.xml jvm.options
# 本地参数
-server -Xms4096m -Xmx4096m -XX:MaxDirectMemorySize=256m -XX:NewRatio=2 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking -XX:+UseCompressedOops -XX:+UseFastAccessorMethods

```

#### 最终成绩
- 85.4
