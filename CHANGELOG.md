**v1.2.0** (2019/09/12)
- 新增 DebugConsumer, 便于调试接口 
- 优化 LoggerConsumer, 支持按小时切分日志文件
- 优化代码，提升稳定性

**v1.1.17** (2019/08/23)
- 优化数据上报异常时异常打印提示
- BatchConsumer 请求异常返回码提醒

**v1.1.16** (2019/05/30) 
- 解决 LoggerConsumer 多线程下会出现关闭不 flush 数据的 bug
- 解决 BatchConsumer 多线程下数据重复的情况

**v1.1.15** (2019/04/28)
- 修复 Java 1.7 兼容性 bug
- LoggerConsumer 不根据时间间隔落盘

**v1.1.14** (2019/04/25)
- 兼容 Java 1.7
- 优化了loggerConsumer的上报机制

**v1.1.13** (2019/04/11)
- 优化数据上报的性能及稳定性
- 调整了Consumer 的默认参数
