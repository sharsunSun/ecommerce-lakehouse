# ecommerce-lakehouse

电商湖仓示例项目，使用 Databricks Bundle 管理 Unity Catalog Schema、Serverless Lakeflow Pipeline 和 Job，通过 Python wheel 分发独立任务与 SQL 模板。

本说明基于历史 README 恢复，并同步当前调度、维护和本地连接配置。四类 YAML 的详细职责见 [Databricks 配置指南](databricks-config-guide.md)。

## 数据链路

```mermaid
flowchart LR
	B[Bronze 原始 Delta 表] --> S[Silver 清洗与去重]
	S --> G[Gold 明细与聚合]
	G --> M[Unity Catalog 指标视图]
```

- Bronze 输入为 `orders_raw`、`users_raw`、`products_raw`、`clickstream_raw`。生产采集由外部链路提供，项目不自动接入生产数据源。
- Silver 按主键保留采集时间最新的记录，同时间戳使用内容摘要确定性打破平局；不合法记录排除，原始数据仍保留在 Bronze。
- Gold 生成订单宽表、日销售额和商品热度聚合，保留未匹配维度的订单，并通过质量约束记录维度缺失。
- 订单示例采用一行一个订单模型，仅支持 CNY，金额使用 Decimal。多商品订单和多币种需要扩展模型。
- 销售指标排除退款订单。跨日买家数需从明细去重，客单价按总金额除以去重订单数计算，不能直接累加每日买家数或平均每日客单价。
- Semantic 层发布 Metric View 定义，并不是每天复制一份指标数据；Gold 数据变化后通常不需要重新发布定义。

本项目直接读写 Unity Catalog 表，不依赖 Volume。Schema 中预留了 RAG 层，但不表示已实现 RAG 处理链路。

## 项目结构

| 路径 | 职责 |
| --- | --- |
| [databricks.yml](databricks.yml) | Bundle 入口、环境变量、构建和同步规则 |
| [resources/schemas.yml](resources/schemas.yml) | 声明五个 Schema，并配置删除保护 |
| [resources/pipeline.yml](resources/pipeline.yml) | Serverless Pipeline、源码入口和 Spark 配置 |
| [resources/jobs.yml](resources/jobs.yml) | 初始化、数据刷新、语义发布和维护 Job |
| [resources/demo.yml](resources/demo.yml) | 仅 dev 部署的模拟数据与演示编排 |
| [src/pipelines](src/pipelines) | 薄 Lakeflow 入口，负责读表、质量约束和数据集注册 |
| [src/ecommerce_lakehouse/transforms](src/ecommerce_lakehouse/transforms) | 独立于 Lakeflow 装饰器的 Silver/Gold DataFrame 转换 |
| [src/ecommerce_lakehouse/jobs](src/ecommerce_lakehouse/jobs) | 初始化、语义发布和 Delta 维护入口 |
| [src/ecommerce_lakehouse/sql](src/ecommerce_lakehouse/sql) | 随 wheel 分发的 SQL 模板 |
| [scripts/demo](scripts/demo) | dev 模拟数据生成脚本，不包含在生产 wheel 中 |
| [tests/unit](tests/unit) | 配置、SQL 和任务控制测试，以及手动 Connect 测试脚本 |
| [tests/integration](tests/integration) | 真实 Spark 转换测试 |
| [.github/workflows/validate.yml](.github/workflows/validate.yml) | GitHub Actions 验证和可选云端演示 |

[pyproject.toml](pyproject.toml) 管理包元数据、依赖和三个命令入口。Job 安装并运行 wheel；Pipeline 将同步源码的 `src` 设置为根目录，复用同一套转换模块。

SQL 文件包含 Python 模板占位符，由 [sql_resources.py](src/ecommerce_lakehouse/sql_resources.py) 渲染后执行，不能原样提交到 SQL 编辑器。指标首次创建后使用 `ALTER VIEW` 更新，以避免删除重建造成授权丢失。

## 环境与资源

| 配置 | dev | prod |
| --- | --- | --- |
| Bundle 模式 | `development`，默认目标 | `production` |
| Catalog | 已有的 `workspace` | 平台预先提供的 `ecommerce_lakehouse` |
| Schema 基础名称 | `ecommerce_bronze_dev` 等 | `bronze`、`silver`、`gold`、`semantic`、`rag` |
| 部署目录 | 当前用户目录下的 Bundle 路径 | `/Shared/prod/bundles/ecommerce-lakehouse-prod` |
| 运行身份 | 当前开发配置使用个人身份 | `prod_run_as_sp` 指定的 Service Principal |

开发模式可能为 Schema 名称添加前缀。查看表时应以 Bundle 解析后的资源名称为准，不要直接假设变量中的基础名称就是实际名称。

Catalog 是平台提供的前置基础设施，本项目不创建或接管 Catalog。Schema 由 Bundle 声明管理，`prevent_destroy: true` 用于阻止部署工具销毁 Schema，不是数据备份，也不能防止外部手动删除。

Pipeline 和 Python Job 均使用 Serverless，不需要配置经典集群。Job 的 `serverless_environment_version` 默认值为 `"4"`，不是 Databricks Connect 包版本，也不直接控制 Pipeline 后端运行时。实际可用环境与 Metric View 能力需在目标工作区确认。

## 本地环境与 Databricks Connect

在项目根目录准备虚拟环境和开发依赖：

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements-dev.txt
```

如果 `python` 或 `python3` 有指向系统 Python 的 alias，激活虚拟环境不一定覆盖 alias。后续安装和运行优先显式使用 `./.venv/bin/python`，不要向系统 Python 强行安装依赖。

本项目已验证过 Databricks Connect `17.3.15` 与 Python 3.12 的本地连接。Connect 尚未声明在项目依赖组中，需要本地远程开发时单独安装：

```bash
./.venv/bin/python -m pip install 'databricks-connect==17.3.15'
```

该版本是当前已验证的客户端基线，不代表所有工作区和后续环境都必须使用该版本。

在 VS Code 中选择项目 `.venv` 解释器，并连接 Databricks 工作区。项目已通过 [.vscode/settings.json](.vscode/settings.json) 的 `python.envFile` 加载 [.vscode/databricks.env](.vscode/databricks.env)，设置：

```dotenv
GRPC_DNS_RESOLVER=native
```

这使 gRPC 使用系统 DNS 解析器，解决本机曾出现的 `Could not contact DNS servers` 问题。它不修改系统 DNS，也不是免费版账号限制的绕过方式；插件会将变量合并到自动生成的环境文件，不要手动维护该生成文件。修改后需重启已有交互内核。

普通 shell 或所有第三方运行器不会自动读取 `python.envFile`。手动连接脚本还保留了进程内的备用设置，可运行：

```bash
./.venv/bin/python -u tests/unit/test_ds_connect.py
```

该脚本使用 `free` profile 和 Serverless，只显示 0 至 4，不写表。认证配置名不同需自行调整；远程测试可能消耗额度。

## 认证与开发部署

前提是近期版本 Databricks CLI、有效工作区认证，以及创建 Schema、运行 Serverless Pipeline/Job 所需的权限。开发工作区地址配置在 Bundle 的 dev 目标中；换工作区时需同步调整配置与认证。

```bash
databricks auth login --host "https://你的工作区域名" --profile free
databricks current-user me --profile free
databricks bundle validate -t dev --profile free
PATH="$PWD/.venv/bin:$PATH" databricks bundle deploy -t dev --profile free
```

部署命令显式把虚拟环境加入 `PATH`，使 Bundle 的 `python3 -m build --wheel` 子进程使用已安装构建依赖的环境。

`validate` 校验与解析配置，`deploy` 构建并发布资源，`run` 才立即执行指定任务。部署不会立即执行三个 wheel 任务，但会安装启用的调度，后续到时间会自动运行。

首次部署会声明创建 Schema，无需先运行 `initialize_lakehouse`。在启动数据刷新之前，需要准备 Bronze 源表；避免让已启用的调度先于数据准备触发。

## 开发演示与数据保护

dev 专属演示执行顺序为：生成模拟数据，再运行 `ecommerce_refresh`，最后运行 `semantic_refresh`。

```bash
databricks bundle run -t dev generate_mock_data --profile free
```

模拟数据要求目标 Catalog 或 Schema 名称以 `_dev` 结尾，默认拒绝覆盖已有表。确认允许覆盖开发数据后，才显式重置：

```bash
databricks bundle run -t dev generate_mock_data --profile free --params reset_demo_data=true
```

演示支持 `seed` Job 参数；同一日期和随机种子可复现相同业务数据，脚本还支持 `--data-date YYYY-MM-DD`。生产目标不包含模拟数据 Job，不会在日常刷新时覆盖 Bronze。

## Job 调度与维护

| Job | 当前触发方式 | 职责 |
| --- | --- | --- |
| `initialize_lakehouse` | 手动，无调度 | 独立初始化或修复 Schema；不是正常部署前置步骤 |
| `ecommerce_refresh` | 每天北京时间 08:00 | 刷新 Silver/Gold Pipeline，不执行语义 DDL |
| `semantic_refresh` | 手动或发布流程，无调度 | Gold 表存在后首次发布，指标定义或相关结构变化后重新发布 |
| `delta_maintenance` | 每天北京时间 22:00 | 对指定 Bronze 表执行 `OPTIMIZE` 和七天保留期的 `VACUUM` |

两个调度均使用 `Asia/Shanghai`，状态为 `UNPAUSED`，配置位于公共 Job 定义中，因此部署到 dev 或 prod 都会应用。配置修改需要再次部署才能生效。

四个 Job 没有相互串联；文件排列顺序不代表执行顺序。每个 Job 的并发数为 1，并配置排队、超时和重试，但不同 Job、外部采集和手动重置之间仍可能并发，需要协调写入窗口和调度。

维护任务对 `orders_raw`、`users_raw`、`products_raw`、`clickstream_raw` 执行维护，跳过带 Pipeline 属性的对象。Silver/Gold 的 Pipeline 管理数据集不由该任务手动清理。

当前 Job 显式传入 `--vacuum --retention-hours 168`。这是历史数据文件的保留期，不是按业务日期删除七天前的记录。`VACUUM` 删除已不再被当前表引用且超过保留条件的文件，可能限制依赖这些文件的时间旅行或恢复。

启用前确认七天覆盖最长读取、写入事务、流式延迟和回补需求。独立维护函数仍默认不开启 `VACUUM`、默认保留 336 小时，并拒绝低于 168 小时；该安全默认值与此 Job 的显式参数不同。如果已有 Predictive Optimization 覆盖这些表，应避免重复安排维护。

手动执行维护会触发实际清理，务必先确认目标：

```bash
databricks bundle run -t dev delta_maintenance
```

## 生产发布

平台团队应先准备 Catalog、存储、部署权限和运行身份。通过 `BUNDLE_VAR_prod_run_as_sp` 提供 Service Principal 的应用 ID；它不是密钥，也不等同于 CLI 登录凭据或数据授权。

```bash
databricks bundle validate -t prod
PATH="$PWD/.venv/bin:$PATH" databricks bundle deploy -t prod
```

生产工作区连接需另行提供。部署后，在真实采集已填充 Bronze 表的前提下运行首次数据刷新，再发布指标定义：

```bash
databricks bundle run -t prod ecommerce_refresh
databricks bundle run -t prod semantic_refresh
```

生产采用最小权限、工作负载身份和必要的发布审批。Metric View 更新身份需具有相应所有权或权限；结构不兼容的变更应安排迁移窗口。

不要让多个 Bundle 管理同一个 Schema。对于已存在且不由本部署管理的资源，应先核实所有权，并按实际情况使用部署绑定流程，不能通过删除数据容器解决冲突。普通 Delta 表也不能假设可直接被同名 Pipeline 物化视图接管。

## 本地测试与 CI

在准备好开发依赖和 Connect 的项目环境中运行：

```bash
./.venv/bin/python -m unittest discover -s tests/unit -v
./.venv/bin/python -m build --wheel
```

业务单元测试使用 mock 和 SQLite，不执行真实 Spark 运算。注意手动脚本 `test_ds_connect.py` 顶层导入 Databricks Connect，因此对整个 `tests/unit` 执行 discover 时仍需该依赖；`__main__` 保护保证收集时不主动连接远端。

真实 Spark 集成测试需要 Java 17 或 21、PySpark 4.0。应使用独立虚拟环境，避免与 Databricks Connect 提供的 PySpark 冲突：

```bash
python3 -m venv .venv-spark
./.venv-spark/bin/python -m pip install -e '.[dev,spark]'
./.venv-spark/bin/python -m unittest discover -s tests/integration -v
```

该步骤需要下载依赖，不属于部署的自动前置步骤。独立测试环境应放在 Git 忽略的位置；当前忽略规则为 `.venv/`，如使用 `.venv-spark`，需自行追加相应忽略规则。

Spark 测试覆盖去重、Decimal 类型、维表关联和聚合等逻辑，但不能替代 Lakeflow 装饰器、Unity Catalog 权限、Serverless 和 Metric View DDL 的工作区验收。

[GitHub Actions 工作流](.github/workflows/validate.yml) 定义 PR/main 推送时的单元、打包和 Spark 检查，以及手动的 dev Bundle 校验。当前手动 Connect 脚本的顶层依赖未纳入 `dev` 依赖组，因此不能仅凭工作流存在就认定干净 CI 环境必然通过；需单独处理依赖或测试收集范围。

手动启用 `run_dev_smoke` 后才会部署并运行开发演示。云端阶段需要 GitHub `databricks-dev` 环境中的 `DATABRICKS_HOST`、`DATABRICKS_CLIENT_ID` 变量，以及 Databricks 对该工作流的 OIDC 联合认证与授权；本机 VS Code 登录不会自动传给 GitHub Actions。

## 后续完善方向

- 配置失败通知、数据新鲜度、质量阈值和外键缺失率监控。
- 按真实查询模式和文件规模评估维护频率、自动优化及 liquid clustering。
- 当前为批式快照物化视图，是否增量刷新及其成本应观察实际事件与执行计划，不作默认保证。
- 大规模更新接入带业务版本号的 CDC；维度历史需求采用 SCD2，避免用采集时间替代业务版本。
- 扩展多商品订单、多币种模型，并补充业务数据和权限验收。
- 将源码、配置和文档提交到 Git；虚拟环境、构建产物和包元数据保持忽略，避免未提交文件丢失后只能依赖本地历史恢复。
