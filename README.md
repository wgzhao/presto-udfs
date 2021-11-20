# Presto User-Defined Functions(UDFs)

Presto/Trino 自定义函数，当前仅针对 [Trino/PrestoSQL](https://trino.io) 版本有有效， facebook版的 [prestodb](https://prestodb.io) 暂不支持。

## Trino/Presto 版本兼容性

PrestoSQL 的版本终止在 `348` 版本，之后的版本取名为 `trino` ，两者不在兼容。 如果你在使用 `348`及之前的版本，请使用 `2.0.8` 版本，否则选择大于 `2.0.8` 版本的代码

| Presto/Trino 版本  | UDF 版本 |
| ------------- |:-------------:|
| <= 348 |   2.0.8  |
| > 348  | > 2.0.8 | 

## 使用方法

### 直接下载

如果不想编译，可以从 `release` 页面中找到对应的版本的压缩文件下载到本地。或者你也可以按照以下方式进行编译

### 自行编译

下载代码并编译打包

```shell
git clone https://github.com/wgzhao/presto-udfs
mvn clean package assembly:single
```

执行上述指令后，将在 `target` 目录下，生成一个 `udfs-<version>-release.zip` 的压缩包

### 安装

假定你的 Presto 安装  `/usr/lib/presto` 目下，执行下面的命令进行安装

```shell
unzip -q -o udfs-<version>-release.zip -d /usr/lib/presto/plugin/
```

如果安装的是 Trino， 假定安装目录为  `/usr/lib/trino`， 则执行下面的命令：

```shell
unzip -q -o udfs-<version>-release.zip -d /usr/lib/trino/plugin/
```

上面命令完成后，将会在安装目录的 `plugin/` 目录下创建一个 `extra` 目录，所有需要的jar文件均在该目录。

重启 Presto/Trino 服务，连接 Presto/Trino 服务后，执行 `show functions like 'udf_%'` 将得到类似如下的输出

|       Function       | Return Type |  Argument Types  | Function Type | Deterministic |                      Description  |
----------------------|--------------|------------------|---------------|---------------|-----------------------------------------------------|
| udf_add_normal_days  | varchar     | varchar, integer | scalar        | true          | add days from base date |
| udf_add_trade_days   | varchar     | varchar, integer | scalar        | true          | add days from base date with yyyyMMdd format |
| udf_ch2num           | bigint      | varchar          | scalar        | true          | convert chinese number to Arabia number |
| udf_count_trade_days | integer     | varchar, varchar | scalar        | true          | count the number of trade date between given two dates |
| udf_eval             | double      | varchar          | scalar        | true          | the implement of javascript eval function |
| udf_get_birthday     | integer     | varchar          | scalar        | true          | Extract birthday from valid ID card |
| udf_int2ip           | varchar     | integer          | scalar        | true          | get region from IP Address |
| udf_ip2int           | bigint      | varchar          | scalar        | true          | get region from IP Address |
| udf_ip2region        | varchar     | varchar          | scalar        | true          | get region from IP Address |
| udf_ip2region        | varchar     | varchar, varchar | scalar        | true          | get region from IP Address |
| udf_is_idcard        | boolean     | varchar          | scalar        | true          | Check IdCard is valid or not |
| udf_is_trade_date    | boolean     | varchar          | scalar        | true          | is close day or not |
| udf_last_trade_date  | varchar     | varchar          | scalar        | true          | get the last exchange day before specified date |
| udf_max_draw_down    | double      | varchar          | scalar        | true          | get the max drawdown rate |
| udf_num2ch           | varchar     | bigint           | scalar        | true          | convert Chinese number to Arabia number |
| udf_num2ch           | varchar     | bigint, boolean  | scalar        | true          | convert Chinese number to Arabia number |
| udf_num2ch           | varchar     | varchar          | scalar        | true          | convert Chinese number to Arabia number |
| udf_num2ch           | varchar     | varchar, boolean | scalar        | true          | convert Chinese number to Arabia number |
| udf_pmod             | bigint      | bigint, bigint   | scalar        | true          | Returns the positive value of a mod b. |
| udf_pmod             | double      | double, double   | scalar        | true          | Returns the positive value of a mod b |
| udf_to_chinese       | varchar     | varchar          | scalar        | true          | convert number string to chinese number string |
| udf_to_chinese       | varchar     | varchar, boolean | scalar        | true          | convert number string to chinese number string |
| udf_xpath            | array(varchar(x)) | varchar(x), varchar(y) | scalar        | true          | Returns a string array of values within xml nodes that match the xpath
| udf_xpath_boolean    | boolean           | varchar(x), varchar(y) | scalar        | true          | Evaluates a boolean xpath expression
| udf_xpath_double     | double            | varchar(x), varchar(y) | scalar        | true          | Returns a double value that matches the xpath expression
| udf_xpath_float      | double            | varchar(x), varchar(y) | scalar        | true          | Returns a double value that matches the xpath expression
| udf_xpath_int        | bigint            | varchar(x), varchar(y) | scalar        | true          | Returns a long value that matches the xpath expression
| udf_xpath_long       | bigint            | varchar(x), varchar(y) | scalar        | true          | Returns a long value that matches the xpath expression
| udf_xpath_number     | double            | varchar(x), varchar(y) | scalar        | true          | Returns a double value that matches the xpath expression
| udf_xpath_str        | varchar           | varchar(x), varchar(y) | scalar        | true          | Returns a string value that matches the xpath expression
| udf_xpath_string     | varchar           | varchar(x), varchar(y) | scalar        | true          | Returns a string value that matches the xpath expression

## 已经实现的 UDF

### 数学函数

#### udf_pmod(n, m) -> [same as input]

返回 n mod m 的值，商取正数

```sql
select udf_pmod(17, -5) -- -3
```

#### num2ch(string str, [boolean flag]) -> string , num2ch(long num, [ boolean flag]) -> string

把阿拉伯数字转为汉字数字，数字汉字有两种写法，一种是普通写法，一种是大写写法：比如 1 ,普通写作 `一`, 大写则为 `壹`。
`flag` 用来指定采取何种写法，`true` 表示普通写法，`false` 表示大写写法，默认值为 `false`

```sql
select udf_num2ch('103543'); -- 拾万叁仟伍佰肆拾叁
select udf_num2ch(103543, true); -- 十万三千五百四十三
select udf_num2ch(103_543); -- 十万三千五百四十三
select udf_num2ch(); -- NULL
```

### 字符串函数

#### udf_ch2num(string str) -> long, ch2num(long a) -> long

返回中文标记的数字的阿拉伯数字形式 ，如果传递字符串为空或包含非汉字数字，则返回为 NULL。

```sql
select udf_ch2num('一十万三千五百四十三'); -- 103543
select udf_ch2num('壹拾万叁仟伍佰肆拾叁'); -- 103543   
select udf_ch2num(''); -- NULL
select udf_ch2num(null); -- NULL
select udf_ch2num('abc'); -- NULL
```

**注意**:

1. 目前实现的限制，如果是十万XXX这种形式会报错，必须写成一十万
2. 但如果一个不合法的汉字数字，目前无法正确识别, 比如 `select udf_ch2num('拾万万'); -- 10` 得到的是一个不正确的结果

#### udf_to_chinese(string str, [boolean flag] ) -> string

把数字字符串转为汉字字符串，注意它和 `udf_num2ch` 函数区别是本函数不含数量关系，即仅仅把每个阿拉伯数字转为中文汉字。 同样的，使用 `flag` 来区分是普通大写，还是汉字大写。true` 表示普通写法，`false` 表示大写写法，默认值为 `false`

```sql
select udf_to_chinese('2002'); -- 贰零零贰
select udf_to_chinese('2002', false); -- 贰零零贰
select udf_to_chinese('2002', true); -- 二〇〇二
```

#### eval(string str) -> double

实现Javascript中eval函数的功能， 暂时仅支持 `+`，`-`，`*`， `/` 运算

```sql
select udf_eval('4*(5+2)'); -- 28
select udf_eval(null); -- NULL
select udf_eval('ab'); -- NULL
```

#### udf_is_idcard(string id) -> bool

检测给定的身份证号码是否有效， 支持中国大陆身份证（15位和18位）以及港澳台的10位证件号码

```sql
select udf_is_idcard(null); -- false
select udf_is_idcard('23070719391110007X'); -- true
select udf_is_idcard('230707391110007'); -- true
select udf_is_idcard('1234566'); -- false
```

#### get_birthday(string id) -> int

从有效的身份证号码中提取生日，如果身份证无效，则返回为0

```sql
select udf_get_birthday(null); -- 0
select udf_get_birthday('23070719391110007X'); -- 19391110
select udf_get_birthday('230707391110007'); -- 19391110
select udf_get_birthday('1234566'); -- NULL
```

### IP 相关函数

#### udf_ip2int(string ip) -> int

将有效的IP地址转为长整数表达法，如果指定的IP地址无效，则返回为0

```sql
select udf_ip2int('127.0.0.1'); -- 2130706433
select udf_ip2int('0.0.0.0'); -- 0
select udf_ip2int('a.b.c.d'); -- 0
```

#### udf_int2ip(int a) -> string

将长整数表示的IP地址转为十进制字符出表达法，如果`a` 小于0 ，则返回为 NULL

```sql
select udf_int2ip(185999660); -- 11.22.33.44
select udf_int2ip(0); -- 0.0.0.0
select udf_int2ip(-1); -- NULL
```

#### udf_ip2region(string ip, [string flag]) -> string

`ip2region` 实现了IP地址归属地以及国家对应的国际编码查询，国际编码定义来源于 [ISO 3166-1](https://zh.wikipedia.org/wiki/ISO_3166-1)。
该函数带一个必选参数和一个可选参数，语义如下：

```
udf_ip2region(ip, [country|g|province|p|city|c|isp|i|en|m2|m3|digit])
```

必选参数 `ip` 表示要查询的 IP 地址，目前仅支持点分字符串IP地址格式，比如

```sql
select udf_ip2region('119.29.29.29'); -- 中国|0|北京|北京市|腾讯
```

上述查询结果的层级用 `|` 分隔，从第一列开始，分别表示 `国家|区域|省份|城市|供应商(ISP)`

如果对应的列没有值，则用 `0` 表示（注意：不是用`null`表示)

比如下面的查询:

```sql
select udf_ip2region('1.1.1.1'); -- 澳大利亚|0|0|0|0
```

则表示只有国家信息，其他信息缺失

如果IP地址非法，则返回`null`，比如

```sql
select udf_ip2region('a.b.c.d'); -- NULL
select udf_ip2region('1.1.1.'); -- NULL
```

这里的IP地址也可以网络地址，比如

```sql
select udf_ip2region('119.29.29.0'); -- 中国|0|北京|北京市|腾讯
select udf_ip2region('119.29.0.0'); -- 中国|0|广东省|广州市|电信
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
select udf_ip2region('119.29.29.29', 'c'); -- 北京市
select udf_ip2region('8.8.8.8', 'g'); -- 美国
select udf_ip2region('223.5.5.5', 'i'); -- 阿里云
select udf_ip2region('1.1.1.1', 'en') -- Australia
select udf_ip2region('1.1.1.1', 'm2'); -- AU
select udf_ip2region('1.1.1.1', 'm3'); -- AUS
select udf_ip2region('1.1.1.1', 'digit'); -- 36
```

### mobile2region

`mobile2region` 实现了手机号码归属地查询，该函数接受一个必选参数和一个可选参数，语义如下：

```
udf_mobile2region(tel, [province|p|city|c|isp|i])
```

第一个参数 `tel` 表示要查询的手机号码，第二个参数表示要返回的层级，含义如下：

- `province|p` 表示查询IP地址所在省份/州/道
- `city|c` 表示查询IP地址所在城市
- `isp|i` 表示查询

以下是一些查询例子

```sql
select udf_mobile2region('13410774560'); -- 广东|深圳|中国移动
select udf_mobile2region('13011'); -- NULL
select udf_mobile2region('18945871234', 'p'); -- 黑龙江
select udf_mobile2region('18945871234', 'c'); -- 伊春
select udf_mobile2region('18945871234', 'i'); -- 中国电信
```

### 证券交易日相关函数

这里的函数都和中国大陆证券交易日相关的函数，国内证券交易日符合以下条件

1. 双休日和国家法定节假日必然不是交易日
2. 调休中遇到双休日（比如周六要求上班）也不是双休日

由于每年的调休不同，也就导致证券交易日没有固定的规律，需要有交易所在头一年年底下发到各券商，同时遇到一些特别情况，还有临时调整（比如2020年1月31日周五，农历初七，本应为交易日，但受疫情影响，调整为非交易日）。因此交易日之间的计算是证券相关数据分析必然会遇到的问题。下面的函数试图简化交易日期计算难度。

#### udf_add_normal_days(string base_date, int n) -> string

计算在给定日期后的 n 天内的第一个交易日, 如果 n 是正数，则往后计算；如果是负数，则往前计算。

该函数的计算分成两步：

1. 在指定的日期 `base_date`上增加 `n` 天，得到一个日期 `delta_date`；

第二步是找到不超过 `delta_date` 日期的最近交易日。

```sql
select udf_add_normal_days('20210903', 4); -- 20210907
select udf_add_normal_days('20210904', 1); -- 20210903
select udf_add_normal_days('20210906', -1); -- 20210903
```

#### udf_count_trade_days(string d1, string d2) -> int

计算两个给定的第一个日期（包括）和第二个日期（包括）之间有多少个交易日，日期格式为 `yyyyMMdd`

```sql
select udf_count_trade_days('20210901', '20210906'); -- 4
select udf_count_trade_days('20210906', '20210906'); -- 1
select udf_count_trade_days('20210903', '20210906'); -- 2

```

#### udf_add_trade_days(string base_date, int n) -> string

计算在超过指定日期(`base_date`)的最近交易日上增加 n 个交易日后的日期并返回。这个计算实际上两个步骤

1. 找到不超过 `base_date` 日期最近的交易日（`base_date` 如果本身是交易日，则为自身），然后
2. 增加 n 个交易日（注意：不是自然日），等于找到指定日期后几个交易日期

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
select udf_is_trade_date(null); -- false
select udf_is_trade_date(''); -- false
select udf_is_trade_date('20210906'); -- true
select udf_is_trade_date('20210904'); -- false
select udf_is_trade_date('20210904'); -- false
```

### xpath 相关函数

这是一组用来使用 `xpath` 表达式来分析 xml 字符串的函数，其代码来自 [Apache Hive](https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/xml/)

具体的用法，可以参考 [LanguageManual XPathUDF](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+XPathUDF)

要注意的是，相对 Hive 的函数名而言，这里的函数都增加了 `udf_` 前缀

这里列出函数的基本用法

```sql
select udf_xpath('<a><b>b1</b><b>b2</b></a>','a/*'); -- []
select udf_xpath('<a><b>b1</b><b>b2</b></a>','a/*/text()'); -- [b1, b2]
select udf_xpath('<a><b id="foo">b1</b><b id="bar">b2</b></a>','//@id'); -- [foot, bar]
SELECT udf_xpath_string('<a><b>bb</b><c>cc</c></a>', 'a/b'); -- bb
SELECT udf_xpath_string ('<a><b>bb</b><c>cc</c></a>', 'a'); -- bbcc
SELECT udf_xpath_boolean ('<a><b>b</b></a>', 'a/b'); -- true
SELECT udf_xpath_boolean ('<a><b>b</b></a>', 'a/c'); -- false
SELECT udf_xpath_int('<a>b</a>', 'a = 10'); -- 0
SELECT udf_xpath_int('<a><b class="odd">1</b><b class="even">2</b><b class="odd">4</b><c>8</c></a>', 'sum(a/*)'); -- 15
```