# Kafka 用户管理（Users 页）使用说明

在集群侧边栏的 **Users** 页（`/ui/clusters/<集群名>/users`）可以管理 Kafka 的 SCRAM 用户：创建用户、重置密码、删除用户，并在创建时顺带授予 ACL 权限。

> 这里的"用户"指 **SCRAM 凭证**（SASL/SCRAM-SHA-256 / SCRAM-SHA-512）。只有 ACL、没有 SCRAM 凭证的 principal 不会出现在 Users 列表里，它们仍在 **ACL** 页管理。

## 1. 前置条件

### 1.1 集群类型

| 集群类型 | 如何管理凭证 | 额外配置 |
|---|---|---|
| 使用 Admin API 的 Kafka（SCRAM 凭证 Admin API，KIP-554，Kafka 2.7+） | 通过 AdminClient 的 `describe/alterUserScramCredentials` | 无 |
| ZooKeeper 模式 Kafka | 直接读写 ZooKeeper 的 `/config/users/<用户>`，并追加 `/config/changes/config_change_` 通知，让 broker 重新加载 | 必须配置 `zookeeperConnect` |

是否走 ZooKeeper 完全由 `zookeeperConnect` 是否配置决定：**配置了就走 ZooKeeper，没配置就走 Admin API**。ACL 的读写在两种模式下都通过 Admin API。

### 1.2 集群配置项

`application.yml`：

```yaml
kafka:
  clusters:
    - name: my-cluster
      bootstrapServers: broker1:9092,broker2:9092
      # 仅 ZooKeeper 模式需要；有 chroot 时要带上，例如 zk1:2181,zk2:2181/kafka
      zookeeperConnect: zk1:2181,zk2:2181/kafka
      # 需要保护的超级用户，见第 5 节
      superUsers:
        - User:admin
        - User:ops
```

环境变量写法（`0` 是集群序号）：

```bash
KAFKA_CLUSTERS_0_ZOOKEEPERCONNECT=zk1:2181,zk2:2181/kafka
KAFKA_CLUSTERS_0_SUPERUSERS=User:admin,User:ops
```

两项也可以在集群配置表单（Kafka Cluster 区块的 *ZooKeeper Connect* 和 *Super Users*）里填，保存后应用会重启生效。

### 1.3 UI 自身的权限

如果集群启用了授权器，kafbat-ui 连接集群所用的 principal 需要有权限修改 SCRAM 凭证（Kafka 要求 `CLUSTER` 资源的 `ALTER` 权限）以及管理 ACL。权限不足时，操作会失败并弹出服务端返回的错误。

## 2. 页面总览

- 右上角 **+ Create User**：创建用户。
- 右上角复选框 **Show system and super users**：默认不勾选，会隐藏超级用户；勾选后显示。
- 列表列：**Username**、**SCRAM mechanisms**、**Password**（Reset Password 按钮）、行末的垃圾桶图标（悬停时出现）。

只读集群（`readOnly: true`）或没有 ACL 编辑权限的账号，Create User、Reset Password 和删除都会被禁用。列表只需要 ACL 的 VIEW 权限。

## 3. 创建用户

点击 **+ Create User**，右侧会滑出表单。

### 3.1 基本信息

| 字段 | 说明 |
|---|---|
| Username | 必填，首尾空格会被去掉。**用户名和 ACL 的 principal（`User:<用户名>`）始终使用同一个值** |
| Password | 必填 |
| Confirm password | 必填，必须与 Password 一致 |
| SCRAM mechanisms | 至少勾选一个。默认勾选 SCRAM-SHA-512，也可以两个都选 |

密码只在提交时发送，之后无法再查看。

### 3.2 Access template（可选的 ACL 授权）

不想立即授权就保持 **No ACL now**。选择模板后，会在创建用户的同时创建 ACL，主机限制固定为 `*`（不限制）。

| 模板 | 授予的权限 | 需要填写 |
|---|---|---|
| No ACL now | 不创建任何 ACL | — |
| Read-only consumer | Topic 的 READ、DESCRIBE；Consumer group 的 READ、DESCRIBE | Topic(s)、Consumer group(s) |
| Producer | Topic 的 WRITE、DESCRIBE、CREATE（不启用 idempotent，不含 Transaction ID） | Topic(s) |
| Read and write | 同时包含上面两种 | Topic(s)、Consumer group(s) |

### 3.3 Topic(s) / Consumer group(s) 的填写方式

和 ACL 页 *Custom ACL* 的 *Matching pattern* 一致：先选 **EXACT** 或 **PREFIXED**，再手填。因为是手填，**资源还不存在也可以先授权**。

| 模式 | 填写 | 效果 |
|---|---|---|
| EXACT | 一个或多个名字，逗号分隔，如 `orders,payments` | 对每个名字创建 LITERAL 类型的 ACL |
| EXACT | `*` | LITERAL `*`，匹配**全部** topic 或 group |
| PREFIXED | 前缀，如 `orders-` | 创建 PREFIXED 类型的 ACL，匹配所有以该前缀开头的资源 |

注意：
- **只有单独的 `*` 才是通配符。** EXACT 里写 `hebi*` 不表示"以 hebi 开头"，而是匹配名字恰好是 `hebi*` 的资源，实际什么都匹配不到。要按前缀授权，请选 PREFIXED 并填 `hebi`。目前表单**不会**拦截这种写法，创建出来的 ACL 不会报错但也不生效。
- PREFIXED 的前缀里不能含 `*`，表单会拦下。
- 选择了需要 Topic 的模板却没填，或选择了需要 Consumer group 的模板却没填，会在**创建用户之前**被拦下，不会留下一个没有权限的空用户。
- `*` 授予的是全部资源，权限范围很大，使用前请确认。

### 3.4 提交后的结果

- 全部成功：弹出成功提示，抽屉关闭，列表刷新。
- **用户已创建但 ACL 创建失败**：弹出警告 "Kafka user ... was created"，抽屉保持打开。此时用户已经存在，修正授权设置后再次提交即可重试，会用同样的密码再写一次凭证并重新创建 ACL（重复的 ACL 不会出错）。
- 用户创建本身失败：只显示服务端的错误提示，不会创建 ACL。

## 4. 重置密码

点击某一行的 **Reset Password**，打开 *Reset Kafka User Password* 表单：

- Username 不可修改。
- SCRAM mechanisms 会预先勾选该用户**已有**的机制。
- 填写新密码并确认后提交，只会更新**勾选的**机制的凭证。**取消勾选不会删除该机制的凭证**。
- 重置密码不涉及 ACL，表单里没有 Access template。

当前页面没有"只删除某一种机制凭证"的入口。服务端提供了对应接口（见第 8 节），需要时可以直接调用。

## 5. 超级用户（super users）

### 5.1 为什么要手动配置

Kafka 把 broker 的 `super.users` 配置视为敏感配置，通过 AdminClient 读取时**值会被抹成空**。所以 kafbat-ui 无法从 broker 自动得知超级用户，必须在集群配置里用 `superUsers` 显式声明（见 1.2）。写法兼容：

- 列表，一项一个：`- User:admin`、`- User:ops`
- 一项里用分号或逗号分隔：`User:admin;User:ops`（与 Kafka 自己的 `super.users` 格式相同）

### 5.2 配置后的效果

- **Users 页** 和 **ACL 页** 默认隐藏这些 principal 的用户和 ACL，勾选 *Show system and super users* 才显示。
- **不能修改、重置密码或删除超级用户**，也不能删除超级用户的 ACL，服务端会返回 400 和相应的提示。
- ACL 的 CSV 同步不会删除超级用户的 ACL。

### 5.3 没有配置会怎样

`superUsers` 未配置、且 broker 又不返回值时，过滤和保护**都不会生效**：复选框勾不勾结果一样，超级用户也可以被误删。后端启动后会打印一条 WARN，说明 `super.users` 因敏感而被抹掉，以及应该配置 `kafka.clusters[].superUsers`。

## 6. 删除用户

鼠标悬停在某一行，点击行末的垃圾桶图标，确认后会：

1. **先删除**该用户（`User:<用户名>`）在集群上的**全部 ACL**，不限于通过本页创建的；
2. **再删除**该用户的全部 SCRAM 凭证。

这样排序是为了避免残留：Kafka 的 ACL 不会随用户一起消失，如果凭证先删而 ACL 删除失败，之后用同名新建用户会继承旧权限。现在如果第 2 步失败，用户仍然在列表里，已经没有权限，重试删除即可。

## 7. 常见问题

| 现象 | 原因与处理 |
|---|---|
| 勾选 *Show system and super users* 没有变化 | 没配 `superUsers`，见第 5 节 |
| 创建用户时报 `zookeeperConnect is required ...` | 这个集群被识别为 ZooKeeper 模式却没有可用的 ZooKeeper 地址，检查 `zookeeperConnect` |
| 报 `Timed out connecting to ZooKeeper ...` | kafbat-ui 所在机器连不上 ZooKeeper，检查网络和地址（含 chroot） |
| 报 `Kafka super user cannot be modified/deleted` | 目标是 `superUsers` 里声明的超级用户，设计如此 |
| 创建后新用户登录 Kafka 失败 | 创建凭证只是写入了 SCRAM 凭证。还需要 broker 启用对应的 SASL/SCRAM 机制和监听器；ZooKeeper 模式下 broker 通过 `/config/changes` 通知重新加载凭证，可能需要一点时间 |
| 用户没出现在列表里 | 列表只显示有 SCRAM 凭证的用户；可能被当成超级用户隐藏了 |
| 用户创建成功但提示 ACL 失败 | 见 3.4，修正后重新提交 |

## 8. REST 接口

路径前缀 `/api/clusters/{clusterName}/users`：

| 方法 | 路径 | 说明 | 所需 ACL 权限 |
|---|---|---|---|
| GET | `/` | 列出用户，`showSystemUsers=true` 时包含超级用户 | VIEW |
| POST | `/` | 创建用户或重置所选机制的凭证。Body：`{"username","password","mechanisms":["SCRAM-SHA-512"]}` | EDIT |
| DELETE | `/{username}` | 删除用户的全部 ACL 和凭证 | EDIT |
| DELETE | `/{username}/credentials/{mechanism}` | 只删除一种机制的凭证，如 `SCRAM-SHA-512` | EDIT |

校验失败（空用户名、空密码、没有机制、操作超级用户、不支持的机制）返回 400；删除不存在的用户返回 404。

注意：删除用户是先删 ACL 再删凭证，所以对一个已经没有凭证的用户调用删除，会先清掉它的 ACL，然后才因为找不到凭证返回 404。

## 9. 当前限制

- ACL 的主机限制固定为 `*`，Producer 模板不含 Transaction ID，也不启用 idempotent。需要这些请到 ACL 页创建。
- 页面不能单独删除某一种机制的凭证（接口支持）。
- 页面只创建 `User:` 类型的 principal。
- EXACT 模式下不会校验 `hebi*` 这类写法（见 3.3）。
