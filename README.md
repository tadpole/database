database
========
貌似他它本来就可以跑... 

大家先看看他写的main.cpp(居然用goto...)和client.cpp吧.

### 执行流程
1. create
建表, 从test/schema中读出表的格式(貌似不用改?)
2. train
读取test/statistic, 得到要执行指令的频率. do something.
3. load
从test读取初始数据
4. preprocessing
预处理
5. excute(计时!)
执行test/query中的指令, 目前只能执行select.
6. next(计时!)
得到执行结果的表中下一项
7. close
结束

### TODO
1. 更改select的实现
它用递归写的==, 貌似可以不需要递归.
2. 实现where
只有'>'和'<', 用AND连接的  
格式:
```
SELECT column1 FROM tablename WHERE column1 > number1 AND column2 < number2 AND column3 < number3 ;
```
3. 实现insert
格式:
```
INSERT INTO table VALUES ( value1,value2,value3 ) , ( value1,value2,value3 )…… 
```
4. 在train中优化

> 前3个测试点详见网络学堂前三个测试点数据说明
