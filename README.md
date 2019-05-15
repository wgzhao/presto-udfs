# 代码来源说明

该仓库原始代码来自 Qubole 的[Presto UDF Project](https://github.com/qubole/presto-udfs) 代码，不过很久没有更新了，其适配的 [Presto](https://prestodb.github.io) 版本也很低，所以 fork 过来主要是适配工作环境中的版本，并以工作需要适当增加了一些函数

# Presto User-Defined Functions(UDFs)

通过编写代码，可以为Presto 增加插件，从而实现更多的函数。

## Presto 版本兼容性

| 版本  | 最新兼容发布版本 |
| ------------- |:-------------:|
| _ver 0.208+_  | 当前版本    |

## 已经实现的 UDF

该仓库实现了以下 Presto UDF 函数

### HIVE UDFs

* **日期-时间函数**
  
 1. **to_utc_timestamp(timestamp, string timezone) -> timestamp** 
      把给定的带时区的时间戳转为标准时间。
      比如， to_utc_timestamp('1970-01-01 00:00:00','PST') 返回 1970-01-01 08:00:00.
 2. **from_utc_timestamp(timestamp, string timezone) -> timestamp**
      把给定的标准时间转为给定时区的时间。 
      比如, from_utc_timestamp('2019-04-23 09:37:23', 'Asia/Chongqing') 返回 2019-04-23 17:37:23.000
 3. **unix_timestamp() -> timestamp**
      获得当前时间戳（单位为秒）
 4. **year(string date) -> int**
      从日期或者时间戳字符串中提取年份
      比如，year("2019-01-01 00:00:00") = 2019, year("2019-01-01") = 2019.
 5. **month(string date) -> int**
      从日期或者字符串中提取月份
      比如，month("2019-11-01 00:00:00") = 11, month("2019-11-01") = 11.
 6. **day(string date) -> int**
      从日期或者字符串中提取天
      比如，day("2019-11-01 00:00:00") = 1, day("2019-11-01") = 1.
 7. **hour(string date) -> int**
      从日期或者字符串中提取小时部分 
      比如，hour('2009-07-30 12:58:59') = 12, hour('12:58:59') = 12.
 8. **minute(string date) -> int**
      从日期或者字符串中提取分钟部分
      比如， minute('2009-07-30 12:58:59') = 58, minute('12:58:59') = 58.
 9. **second(string date) -> int**
      从日期或者字符串中提取秒部分
      比如，second('2009-07-30 12:58:59') = 59, second('12:58:59') = 59.
 10. **to_date(string timestamp) -> string**
      把时间戳转为日期字符串
      比如，to_date("2019-04-03 12:00:00") = "2019-04-03"
 11. **weekofyear(string date) -> int**
      返回时间戳字符串的周数
      比如，weekofyear('2019-04-23 09:37:23') = 17, weekofyear('2019-04-23') = 17.
 12. **date_sub(string startdate, int days) -> string**
      从给定的日期字符串中减去给定的天数
      比如，date_sub('2008-12-31', 1) = '2008-12-30'.
 13. **date_add(string startdate, int days) -> string**
      从给定的日期字符串中增加给定的天数
      比如，date_add('2008-12-31', 1) = '2009-01-01'.
 14. **datediff(string enddate, string startdate) -> string**
      计算两个日期的天数差
      比如，datediff('2009-03-01', '2009-02-27') = 2.
 15. **format_unixtimestamp(bigint unixtime[, string format]) -> string**
      把一个unix时间戳转为给定格式的日期字符串. 格式字符串遵循Java日期格式标准，详细情况请参考[这里](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
 16. **from_duration(string duration, string duration_unit) -> double**
      把一个表示时间长度的字符串转为持续的时长数值。字符串表示法遵循 airlift 的 [Duration format](https://github.com/airlift/units/blob/master/src/main/java/io/airlift/units/Duration.java)
      比如，from_duration('4h', 'ms') = 1.44E7.
 17. **from_datasize(string datasize, string size_unit) -> double**
       把给定的容量大小按照给定格式进行转换
       比如，from_datasize('1GB', 'B') = 1.073741824E9.

* **数学函数**
  
 1. **pmod(INT a, INT b) -> INT, pmod(DOUBLE a, DOUBLE b) -> DOUBLE**
      返回 a mod b 的值，商取正数
      比如， pmod(17, -5) = -3.
 2. **rands(INT seed) -> DOUBLE**
      返回0-1之间的随机数，注意，该值用在查询中，是每一行的值都会改变。参数为指定随机种子
      比如， rands(3) = 0.731057369148862 
 3. **bin(BIGINT a) -> STRING**
      返回给定整数的二进制值
      比如，bin(100) = 1100100.
 4. **hex(BIGINT a) -> STRING, hex(STRING a) -> STRING, hex(BINARY a) -> STRING**
      如果给定的参数是一个正数或者一个二进制数，则将其转为十六进制数的字符串形式，如果给定的是一个字符串，则把每个字符先转为对应的十六进制，然后返回结果
      比如，hex(123) = 7b, hex('123') = 7b, hex('1100100') = 64.
 5. **unhex(STRING a) -> BINARY**
      hex 的反向函数
      比如，unhex('7b') = 1111011.
 6. **num2ch(STRING str, [INT flag]) -> STRING , num2ch(long a, [ INT flag]) -> STRING** 
      把数字转为汉字的数字，flag为0，把数字转为对应的汉字，flag为1，转为汉字中的大写数字。默认值为0
      比如，num2ch('103543', 1) = '拾万叁仟伍佰肆拾叁', num2ch(103543) = '十万三千五百四十三'

* **字符串函数**
  
 1. **locate(string substr, string str[, int pos]) -> int** 
      返回从pos位置开始，第一次匹配到给定字符的位置
      比如，locate('si', 'mississipi', 2) = 4, locate('si', 'mississipi', 5) = 7
 2. **find_in_set(string str, string strList) -> int** 
      从用逗号分割的字符串组中找到第一个匹配的字符的位置，任意参数为null，则返回为null；如果第一个参数包含任意多个都好，则返回为0.
      比如，find_in_set('ab', 'abc,b,ab,c,def') = 3.
 3. **instr(string str, string substr) -> int** 
      返回一个字串在字符串中首次出现的位置，任意参数为null时，返回null，如果没有找到，则返回为0，注意，开始字符标记为1而不是0
      比如，instr('mississipi' , 'si') = 4.
 4. **ch2num(string str) -> long, ch2num(long a) -> long**
      返回中文标记的数字的阿拉伯数字形式
      比如，ch2num('十万三千五百四十三') = 103543, ch2num('壹拾万叁仟伍佰肆拾叁') = 103543
      _**注意**: 目前实现的限制，如果是十万XXX这种形式会报错，比如写成一十万_
 5. **eval(string str) -> double**
      实现Javascript中eval函数的功能， 暂时仅支持 +，-，*， / 运算
      比如, eval('4*(5+2)') = 28