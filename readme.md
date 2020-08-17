 #### 提交方式

```shell
rm meta_mgr_java_int.zip
zip -r meta_mgr_java_int.zip src pom.xml jvm.options
# 本地参数
-server -Xms4096m -Xmx4096m -XX:MaxDirectMemorySize=256m -XX:NewRatio=2 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking -XX:+UseCompressedOops -XX:+UseFastAccessorMethods

```

#### TODO

- dio 写
- bucket cache
- thread cache
- 1kb page
- merge write 
- 缓存命中率高的话，可以尝试顺序写
