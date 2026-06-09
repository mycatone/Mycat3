# Mycat3

分布式数据库中间件 | Distributed Database Middleware

---

## 项目简介

Mycat3 是 Mycat 社区开发的新一代分布式关系型数据库中间件。它在 Mycat2 的基础上全面重构，以 **Apache Calcite** 为 SQL 优化引擎，基于 **Vert.x** 异步网络框架，提供高性能的分布式 SQL 查询、数据分片、读写分离和多协议前端支持。

### 核心能力

- **多协议统一端口** — 同一个 8066 端口同时支持 MySQL、TDS (SQL Server)、TNS (Oracle)、PostgreSQL 四种协议，字节级自动检测协议类型
- **分布式查询引擎** — 基于 Apache Calcite 的规则优化 + 代价优化，支持跨分片 JOIN、子查询下推、GROUP BY pushdown
- **数据分片** — 内置 18 种分片算法（取模、MurmurHash、日期范围、CRC32 预分槽等），支持自定义算法扩展
- **读写分离** — 支持多种负载均衡策略（随机、轮询、权重），自动主从切换与心跳检测
- **XA 分布式事务** — 完整 2PC 实现，保障跨分片数据一致性
- **SQL 拦截器** — DSL 驱动的 SQL 改写与路由，支持自定义命令分发

---

## 快速开始

### 环境要求

- JDK 11+（编译至 JDK 8 字节码）
- Maven 3.6+

### 构建

```bash
cd Mycat3
mvn clean compile -f pom.xml
```

### 运行测试

```bash
mvn test -f pom.xml
```

### 配置与启动

Mycat3 使用 JSON 多文件配置，核心配置文件位于 `mycat3/src/main/resources/`：

| 文件 | 用途 |
|------|------|
| `server.json` | 服务端口、线程池、负载均衡 |
| `users/*.user.json` | 用户认证 |
| `datasources/*.datasource.json` | 后端数据源 |
| `clusters/*.cluster.json` | 集群拓扑与读写分离 |
| `schemas/*.schema.json` | 逻辑库与分片表定义 |
| `function.yml` | 分片算法定义 |

快速配置示例（单机 MySQL 后端）：

**server.json**
```json
{
  "mode": "local",
  "server": {
    "ip": "0.0.0.0",
    "port": 8066,
    "reactorNumber": 8
  }
}
```

**用户配置** (`users/root.user.json`)
```json
{
  "username": "root",
  "password": "",
  "schemas": ["db1"],
  "transactionType": "proxy"
}
```

**数据源** (`datasources/ds0.datasource.json`)
```json
{
  "name": "ds0",
  "url": "jdbc:mysql://127.0.0.1:3306/db1",
  "user": "root",
  "password": "",
  "maxCon": 1000,
  "instanceType": "READ_WRITE"
}
```

**集群** (`clusters/prototype.cluster.json`)
```json
{
  "name": "prototype",
  "clusterType": "SINGLE_NODE",
  "masters": ["ds0"]
}
```

**逻辑库** (`schemas/db1.schema.json`)
```json
{
  "name": "db1",
  "targetCluster": "prototype",
  "tables": []
}
```

### 连接

```bash
# MySQL 客户端
mysql -h 127.0.0.1 -P 8066 -u root

# SQL Server 客户端 (需要 encrypt=false)
sqlcmd -S 127.0.0.1,8066 -U sa -No

# PostgreSQL 客户端
psql -h 127.0.0.1 -p 8066 -U root -d db1

# Oracle 客户端
sqlplus root@127.0.0.1:8066/ORCL
```

---

## 项目结构

```
Mycat3/
├── mycat3/         # 主模块：协议实现、统一端口、服务启动
├── proxy/          # 代理层：NIO 网络框架、连接管理
├── router/         # 路由层：分片算法、路由策略
├── executor/       # 执行层：物理执行引擎
├── calcite/        # SQL 优化：Calcite 适配与优化规则
├── hbt/            # HBT 语言：异构查询 DSL
├── config/         # 配置管理：JSON 解析与 Schema 加载
├── common/         # 公共模块：类型系统、工具类
├── datasource/     # 数据源：连接池、心跳检测
├── replica/        # 副本：读写分离、主从切换
├── plug/           # 插件机制
├── statistic/      # 统计与监控
├── console/        # 管理控制台（9066 端口）
├── assistant/      # 辅助工具
└── example/        # 示例配置与测试
```

---

## 多协议前端

Mycat3 在同一端口 (8066) 上自动识别并处理四种数据库协议：

| 协议 | 客户端 | 认证方式 | 状态 |
|------|--------|----------|------|
| MySQL | mysql CLI, JDBC, ODBC, .NET | scramble 411 + Authenticator | 完整支持 |
| TDS (SQL Server) | sqlcmd, SSMS, JDBC | Login7 明文（无 TLS） | 需 `encrypt=false` |
| PostgreSQL | psql, JDBC, pgAdmin | MD5 挑战认证 | 完整支持 |
| TNS (Oracle) | sqlplus, JDBC thin | 简化认证 | PoC 闭环 |

> **统一端口工作原理**：通过分析客户端发来的首个数据包字节特征自动识别协议类型，然后分发到对应的协议处理器。所有协议共享同一套 SQL 解析 → 路由 → 分片 → 执行 → 结果归并的通路。

---

## 分片算法

Mycat3 内置 18 种分片算法，覆盖常见业务场景：

| 算法 | 说明 |
|------|------|
| `PartitionByMod` | 数值取模 |
| `PartitionByRangeMod` | 范围分组 + 组内取模 |
| `PartitionByHashMod` | hashCode 取模 |
| `PartitionByMurmurHash` | Murmur3-32 一致性哈希 |
| `PartitionByJumpConsistentHash` | Jump Hash |
| `PartitionByLong` | 1024 槽位范围映射 |
| `PartitionByDate` | 按天分片 |
| `PartitionByMonth` | 按月分片 |
| `PartitionByMonthAndHistory` | 当月 + 历史归档 |
| `PartitionByLatestMonth` | 最新月份取模 |
| `PartitionByHotDate` | 热数据分片 |
| `PartitionByRangeDateHash` | 日期分组 + 子分片 |
| `PartitionByPattern` | 数值模式匹配 |
| `PartitionByPrefixPattern` | 前缀 ASCII 模式 |
| `PartitionByString` | 字符串哈希 |
| `PartitionDirectBySubString` | 子串直接映射 |
| `PartitionByFileMap` | 枚举值文件映射 |
| `PartitionByCRC32PreSlot` | CRC32 102400 槽预分 |

分片算法在 `router/src/main/resources/function.yml` 中配置，支持自定义扩展。

---


## 相关资源

- 官网：[http://mycatone.top/](http://mycatone.top/)
- 语雀文档：[https://www.yuque.com/books/share/6606b3b6-3365-4187-94c4-e51116894695](https://www.yuque.com/books/share/6606b3b6-3365-4187-94c4-e51116894695)

---

## License

[GNU General Public License v3.0](LICENSE.txt)

Copyright (C) Mycat Community