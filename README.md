# 代码来源说明

该仓库原始代码来自 Qubole 的[Presto UDF Project](https://github.com/qubole/presto-udfs) 代码，不过很久没有更新了，其适配的 [Presto](https://prestodb.github.io) 版本也很低，所以 fork 过来主要是适配工作环境中的版本，并以工作需要适当增加了一些函数

# Presto User-Defined Functions(UDFs)

通过编写代码，可以为Presto 增加插件，从而实现更多的函数。

## Presto 版本兼容性

| 版本  | 最新兼容发布版本 |
| ------------- |:-------------:|
| _ver 0.208+_  | 当前版本    |

## 使用方法

### 下载代码

`git clone https://github.com/wgzhao/presto-udfs`

### 编译

`mvn clean package`

### 安装

假定你的 Presto 安装  _/usr/lib/presto_ 目下， 首先在 _/usr/lib/presto/plugin_ 目录下创建一个文件夹，比如 _/usr/lib/presto/plugin/udfs\_example_ ,
然后把编译出来的 _target/udfs-2.0.4-SNAPSHOT.jar_ 文件拷贝到上述新建目录中。

重启 Presto 服务，连接 Presto 服务后，执行 `show functions` 查看是否有新增的函数，如果没有，可以查看对应的日志查看错误原因。

## 已经实现的 UDF

该仓库实现了以下 Presto UDF 函数

### 日期-时间函数

#### to_utc_timestamp(timestamp, string timezone) -> timestamp

把给定的带时区的时间戳转为标准时间。  

```sql
select to_utc_timestamp('1970-01-01 00:00:00','PST') -- 1970-01-01 08:00:00`
```

#### from_utc_timestamp(timestamp, string timezone) -> timestamp

把给定的标准时间转为给定时区的时间。 

```sql
select from_utc_timestamp('2019-04-23 09:37:23', 'Asia/Chongqing')  -- 2019-04-23 17:37:23.000
```

### 数学函数

 #### pmod(n, m) -> [same as input]

返回 n mod m 的值，商取正数 

```sql
select pmod(17, -5) -- -3
```

#### num2ch(string str, [int flag]) -> string , num2ch(long a, [ int flag]) -> string

把数字转为汉字的数字，flag为0，把数字转为对应的汉字，flag为1，转为汉字中的大写数字。默认值为0 

```sql
select num2ch('103543', 1) -- 拾万叁仟伍佰肆拾叁
select num2ch(103543) -- 十万三千五百四十三
```

### 字符串函数

#### ch2num(string str) -> long, ch2num(long a) -> long

返回中文标记的数字的阿拉伯数字形式 

```sql
select ch2num('十万三千五百四十三'); -- 103543
select ch2num('壹拾万叁仟伍佰肆拾叁'); -- 103543   
```
 **注意**: 目前实现的限制，如果是十万XXX这种形式会报错，比如写成一十万

#### eval(string str) -> double
实现Javascript中eval函数的功能， 暂时仅支持 `+`，`-`，`*`， `/` 运算

```sql
select eval('4*(5+2)'); -- 28
```

#### is_idcard(string id) -> bool

检测给定的身份证号码是否有效， 支持中国大陆身份证（15位和18位）以及港澳台的10位证件号码

```sql
select is_idcard() -- false
select is_idcard('23070719391110007X') -- true
select is_idcard('230707391110007') -- true
select is_idcard('1234566') -- false
```

#### get_birthday(string id) -> int

从有效的身份证号码中提取生日，如果身份证无效，则返回为0

```sql
select get_birthday() -- 0
select get_birthday('23070719391110007X') -- 19391110
select get_birthday('230707391110007') -- 19391110
select get_birthday('1234566') -- 0
```

### IP 相关函数

#### ip_to_int(string ip) -> int

将有效的IP地址转为长整数表达法，如果指定的IP地址无效，则返回为0

```sql
select ip_to_int('127.0.0.1'); -- 2130706433
select ip_to_int('0.0.0.0'); -- 0
select ip_to_int('a.b.c.d'); -- 0
```

#### int_to_ip(int a) -> string

将长整数表示的IP地址转为十进制字符出表达法，如果`a` 小于0 ，则返回为 NULL

```sql
select int_to_ip(185999660); -- 11.22.33.44
select int_to_ip(0); -- 0.0.0.0
select int_to_ip(-1); -- NULL
```

#### ip2region(string ip, [string flag]) -> string

`ip2region` 实现了IP地址归属地以及国家对应的国际编码查询，国际编码定义来源于 [ISO 3166-1](https://zh.wikipedia.org/wiki/ISO_3166-1)。
该函数带一个必选参数和一个可选参数，语义如下：

```sql
ip2region(ip, [country|g|province|p|city|c|isp|i|en|m2|m3|digit])
```

必选参数 `ip` 表示要查询的 IP 地址，目前仅支持点分字符串IP地址格式，比如 

```sql
presto> select ip2region('119.29.29.29'); -- 中国|0|北京|北京市|腾讯
```

上述查询结果的层级用 `|` 分隔，从第一列开始，分别表示 `国家|区域|省份|城市|供应商(ISP)`

如果对应的列没有值，则用 `0` 表示（注意：不是用`null`表示)

比如下面的查询:

```sql
select ip2region('1.1.1.1'); -- 澳大利亚|0|0|0|0
```

则表示只有国家信息，其他信息缺失

如果IP地址非法，则返回`null`，比如

```sql
select ip2region('a.b.c.d');  -- NULL
select ip2region('1.1.1.'); 	-- NULL
```

这里的IP地址也可以网络地址，比如

```sql
select ip2region('119.29.29.0'); -- 中国|0|北京|北京市|腾讯
select ip2region('119.29.0.0');  -- 中国|0|广东省|广州市|电信
```

第二个参数为可选参数，用来指定想要获取哪个层级的信息，每个定义有完整单词以及缩写，含义如下：

- `country|g` 表示查询IP地址所在国家
- `province|p` 表示查询IP地址所在省份/州/道
- `city|c` 表示查询IP地址所在城市
- `isp|i` 表示查询
- `en` 返回IP地址表示国家英语名称，比如 `China`
- `m2` 返回IP地址表示国家两位自字母代码，比如 `CN`
- `m3` 返回IP地址表示国家三位自字母代码，比如 `CHN`
- `digit` 返回IP地址表示国家数字代码，比如 `156`

以下是一些查询例子

```sql
select ip2region('119.29.29.29','c'); -- 北京市
select ip2region('8.8.8.8','g'); 			-- 美国
select ip2region('223.5.5.5','i'); 		-- 阿里云
select ip2region('1.1.1.1','en') 			-- Australia
select ip2region('1.1.1.1','m2'); 		-- AU
select ip2region('1.1.1.1','m3'); 		-- AUS
select ip2region('1.1.1.1','digit'); 	-- 36
```

### mobile2region

`mobile2region` 实现了手机号码归属地查询，该函数接受一个必选参数和一个可选参数，语义如下：

```sql
mobile2region(tel, [province|p|city|c|isp|i])
```

第一个参数 `tel` 表示要查询的手机号码，第二个参数表示要返回的层级，含义如下：

- `province|p` 表示查询IP地址所在省份/州/道
- `city|c` 表示查询IP地址所在城市
- `isp|i` 表示查询

以下是一些查询例子

```sql
select mobile2region('13410774560'); 		-- 广东|深圳|中国移动
select mobile2region('13011'); 					-- NULL
select mobile2region('18945871234','p'); -- 黑龙江
select mobile2region('18945871234','c'); -- 伊春
select mobile2region('18945871234','i'); -- 中国电信
```

###  证券交易日相关函数

这里的函数都和中国大陆证券交易日相关的函数，国内证券交易日符合以下条件

1. 双休日和国家法定节假日必然不是交易日
2. 调休中遇到双休日（比如周六要求上班）也不是双休日

由于每年的调休不同，也就导致证券交易日没有固定的规律，需要有交易所在头一年年底下发到各券商，同时遇到一些特别情况，还有临时调整（比如2020年1月31日周五，农历初七，本应为交易日，但受疫情影响，调整为非交易日）。因此交易日之间的计算是证券相关数据分析必然会遇到的问题。下面的函数试图简化交易日期计算难度。



#### udf_add_normal_days(string basedate, int n) -> string

计算在给定日期后的 n 天内的第一个交易日, 如果 n 是正数，则往后计算；如果是负数，则往前计算。

该函数的计算分成两步：

1. 在指定的日期 `basedate`上增加 `n` 天，得到一个日期 `delta_date`；

第二步是找到不超过 `delta_date` 日期的最近交易日。

```sql
select udf_add_normal_days('20210903', 4); -- 20210907
select udf_add_normal_days('20210904', 1); -- 20210903
select udf_add_normal_days('20210906', -1) -- 20210903
```

#### udf_count_trade_days(string d1, string d2) -> int

计算两个给定的第一个日期（包括）和第二个日期（包括）之间有多少个交易日，日期格式为 `yyyyMMdd`

```sql
select udf_count_trade_days('20210901', '20210906'); -- 4
select udf_count_trade_days('20210906', '20210906'); -- 1
select udf_count_trade_days('20210903', '20210906'); -- 2

```

#### udf_add_trade_days(string basedate, int n) -> string

计算在超过指定日期(`basedate`)的最近交易日上增加 n 个交易日后的日期并返回。这个计算实际上两个步骤

1. 找到不超过 `basedate` 日期最近的交易日（`basedate` 如果本身是交易日，则为自身），然后
2. 增加 n 个交易日（注意：不是自然日），等于找到指定日期的后几个交易日期

```sql
select udf_add_trade_days('20210903', 1); -- 20210906
select udf_add_trade_days('20210904', 1); -- 20210906
select udf_add_trade_days('20210101', 3); -- 20210104
```

注意该函数于 `udf_add_normal_days` 的逻辑区别。

#### udf_last_trade_trade(string d) -> string

获取指定日期(`d`)的上一 日 交易日并返回，如果没有找到，则返回 NULL.

```sql
select udf_last_trade_date('20210906'); -- 20210903
select udf_last_trade_date('20210907'); -- 20210906
select udf_last_trade_date('19920101'); -- NULL
```

#### udf_is_trade_date(string d) -> bool

判断给定的日期是否为交易日，如果是，返回 true，其他情况返回 false

```sql
select udf_is_trade_date(); -- false
select udf_is_trade_date(''); -- false
select udf_is_trade_date('20210906'); -- true
select udf_is_trade_date('20210904'); -- false
select udf_is_trade_date('20211009'); -- false
```

