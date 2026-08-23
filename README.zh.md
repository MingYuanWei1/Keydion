# Keydion - 学术论文管理系统

[English](README.md) | 中文

Keydion 是一款健壮、面向学术的 Web 应用，用于管理、检索和预览学术论文。它基于 Flask 和 MySQL 构建，提供多语言支持、高品质的视觉设计，并为 IB 论文提供专门支持。

## 功能

- **学术检索**：高级检索界面，支持按学科、日期和语言筛选。
- **论文预览**：在浏览器内预览 PDF，配有显示元数据的自定义侧边栏。
- **IB 拓展论文（EE）支持**：为 IB EE、IA、CP 和其他学术论文提供专门的元数据字段。
- **Keydion AI**：在 `/ask` 对资料库进行 RAG（检索增强生成）对话，支持会话、引用、PDF 附件，以及可选的 agent（智能体）网络/文档工具；还提供语义搜索和语义「相关论文」，由存储在 MySQL `VECTOR` 列中的 embedding 支撑。
- **AI（人工智能）辅助元数据**：优先由视觉模型从论文 PDF 中提取摘要、关键词和 EE/IA 评分数据，以 OCR + 文本 LLM（大语言模型）作为回退；未配置 LLM 密钥时，所有 AI 功能均可优雅降级。
- **指南、学术资源与期刊**：已发布的指南、可浏览的学术资源文件树，以及学术期刊列表。
- **投稿与审核工作流**：读者提交论文供审核，贡献者直接发布，管理员管理资料库。
- **多语言支持**：英文与中文的完整国际化（i18n）。
- **双重认证**：支持本地 PBKDF2 哈希密码与 Microsoft Graph OAuth 登录及资料同步，并提供基于角色的访问控制（读者 / 贡献者 / 管理员）。
- **新闻管理**：内置系统，用于发布和管理学术新闻与公告。

## 环境要求

- **Python 3.11+**（生产容器固定使用 Python 3.14）
- **MySQL 9.x**（CI 固定使用官方 `mysql:9.7.1` 镜像）
- **Tesseract OCR**（可选）：支持对*扫描版* PDF 进行本地文本提取（供聊天附件、摘要/关键词生成器和论文索引使用）。安装引擎和中文语言数据：
  - Debian/Ubuntu：`apt-get install -y tesseract-ocr tesseract-ocr-chi-sim`
  - macOS：`brew install tesseract tesseract-lang`
  配置了 LLM 视觉提供方时，系统优先使用视觉读取，本地 Tesseract 提取作为回退。

## 数据库设置

Keydion 通过你的 `.env` / `.env.prod` 中的 `PAPERQUERY_DATABASE_URL` 连接串访问 MySQL。部署之前，你必须创建**数据库**（通常还包括一个专用用户）。运行时启动会校验数据库已处于预期的 Alembic revision；它不会升级非空 schema，也不会自行创建数据库。

以 MySQL 管理员身份连接（`mysql -u root -p`）并执行：

```sql
-- utf8mb4 is required: PDF-extracted text and CJK metadata need 4-byte chars.
CREATE DATABASE keydion CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER 'keydion'@'localhost' IDENTIFIED BY 'change-me';
GRANT ALL PRIVILEGES ON keydion.* TO 'keydion'@'localhost';
FLUSH PRIVILEGES;
```

然后将你的 `.env` 指向该数据库（保留 `?charset=utf8mb4` 查询串）：

```
PAPERQUERY_DATABASE_URL="mysql+pymysql://keydion:change-me@127.0.0.1:3306/keydion?charset=utf8mb4"
```

对于已有安装，在启动新版本之前，请先遵循协同的[论文发布迁移运维手册](docs/deployment/paper-publishing-migration.md)。该手册涵盖预检、备份、基线标记、Alembic 升级、验证、冒烟测试和回滚。语义搜索和 Keydion AI 使用 MySQL 9.x 的二进制 `VECTOR` 列；不受支持的 MySQL 或 schema 形态会直接校验失败，而不是让迁移静默降级。

## 生产部署（systemd 下的 gunicorn，宿主机 nginx）

生产环境直接在 systemd 下运行 gunicorn，由宿主机的 nginx 充当反向代理。Flask 开发服务器（及其 Werkzeug 调试器）**绝不能**对外暴露。

纳入版本管理的 web 单元加载 `/Keydion/.env.prod`，并从共享的 `/Keydion/.venv` 启动 Gunicorn。发布和附件处理运行在独立的工作进程单元中，现行论文的完整性扫描由 systemd timer 运行。nginx 直接从磁盘提供 `/static/*`；PDF 下载路由（`/papers/*`）代理转发到 Flask，以执行鉴权检查。

1. 在仓库根目录创建 `.env.prod`（已被 gitignore）：复制 [`.env.example`](.env.example) 并填入各项值。使用高强度的随机 `PAPERQUERY_SECRET`（切勿使用 `dev-secret-key`），并将 `PAPERQUERY_MS_REDIRECT_URI` 设为你的公开回调 URL（例如 `https://yourdomain.com/auth/callback`）。

2. 在仓库根目录创建虚拟环境并安装依赖：

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install --require-hashes -r requirements.lock
   ```

   对于刚创建、仍完全为空的数据库，请显式执行引导，然后验证唯一的 Alembic head。切勿对已有安装运行引导命令：

   ```bash
   .venv/bin/python -m tools.bootstrap_database --confirm-empty-bootstrap
   .venv/bin/python -m tools.verify_alembic_state
   ```

3. 放置 nginx 站点配置。参考配置位于 [`deploy/keydion.nginx.conf`](deploy/keydion.nginx.conf)；关键在于 `proxy_set_header X-Forwarded-Proto $scheme;`（仅限 TLS 的 vhost 可用 `https`），这样 Flask 的 `ProxyFix` 才能为 OAuth 回调生成正确的 HTTPS URL。

   ```bash
   sudo cp deploy/keydion.nginx.conf /etc/nginx/sites-available/keydion
   sudo ln -s /etc/nginx/sites-available/keydion /etc/nginx/sites-enabled/
   sudo nginx -t && sudo systemctl reload nginx
   ```

   TLS 不在本节范围内：请在 nginx 本身（certbot）或上游负载均衡器终结 HTTPS。

4. 确认宿主机使用 `keydion` 账户、`/Keydion`、`/Keydion/.env.prod` 和 `/Keydion/.venv`。全新部署时，先创建所有非可选的 `ReadWritePaths` 目录，再安装单元；已有部署或涉及 schema 变更的部署，则必须改用迁移运维手册中的存储来源检查。然后安装纳入版本管理的单元：

   `tests/fixtures/deploy/keydion-legacy.service.fixture` 是一份经过评审的首次部署产物，仅供该运维手册使用：当旧版本早于 `deploy/keydion.service` 时，用它识别确切的旧版 web 单元；它绝不会被安装为正式单元。出现不匹配即硬性中止。请在维护窗口之前修改经过评审的 fixture（测试前置数据）、运维手册和部署约定；切勿在窗口期间编辑或复制宿主机上已安装的字节来强行匹配。

   ```bash
   sudo install -d -o keydion -g keydion -m 0750 \
     /Keydion/data /Keydion/data/pending_papers /Keydion/papers \
     /Keydion/resource_files /Keydion/static/uploads /var/log/keydion
   for path in /Keydion/data /Keydion/data/pending_papers /Keydion/papers \
     /Keydion/resource_files /Keydion/static/uploads /var/log/keydion; do
     test "$(stat -c '%U:%G:%a' "$path")" = keydion:keydion:750
   done
   sudo cp deploy/keydion.service /etc/systemd/system/keydion.service
   sudo cp deploy/keydion-publishing-worker.service \
     /etc/systemd/system/keydion-publishing-worker.service
   sudo cp deploy/keydion-attachment-worker.service \
     /etc/systemd/system/keydion-attachment-worker.service
   sudo cp deploy/keydion-paper-integrity.service \
     /etc/systemd/system/keydion-paper-integrity.service
   sudo cp deploy/keydion-paper-integrity.timer \
     /etc/systemd/system/keydion-paper-integrity.timer
   sudo systemctl daemon-reload
   sudo systemd-analyze verify /etc/systemd/system/keydion.service \
     /etc/systemd/system/keydion-publishing-worker.service \
     /etc/systemd/system/keydion-attachment-worker.service \
     /etc/systemd/system/keydion-paper-integrity.service \
     /etc/systemd/system/keydion-paper-integrity.timer
   ```

   web、各工作进程和扫描器使用相同的环境。工作进程是独立进程；Gunicorn 绝不会从 `post_fork` 启动它们。

5. 分别启用工作进程、完整性 timer 和 web 单元：

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now keydion-publishing-worker
   sudo systemctl enable --now keydion-attachment-worker
   sudo systemctl enable --now keydion-paper-integrity.timer
   sudo systemctl enable --now keydion
   sudo systemctl status keydion-publishing-worker --no-pager
   sudo systemctl status keydion-attachment-worker --no-pager
   sudo systemctl status keydion-paper-integrity.timer --no-pager
   sudo systemctl status keydion --no-pager
   ```

   已有数据库必须先完成所链接的迁移运维手册，之后各单元才能在新版本上启动。

### 在 `git pull` 后更新服务器（仅限 schema 不变的版本）

此快捷方式仅适用于数据库 schema 保持不变的版本。对于变更 schema 的版本，不要先在提供服务的 worktree 中 pull 或检出候选版本：请使用协同的[论文发布迁移运维手册](docs/deployment/paper-publishing-migration.md)，该手册会在检出之前隔离服务并划定恢复边界。对于 schema 不变的版本，请根据实际变更内容选择命令：

| 变更 | 命令 |
|---|---|
| Python 代码（`app.py`、模板、工作进程/服务） | `sudo systemctl restart keydion-publishing-worker keydion-attachment-worker && sudo systemctl reload keydion` |
| `requirements.lock` | `.venv/bin/pip install --require-hashes -r requirements.lock && sudo systemctl restart keydion-publishing-worker keydion-attachment-worker keydion` |
| `gunicorn.conf.py` 或 `.env.prod` | `sudo systemctl restart keydion-publishing-worker keydion-attachment-worker keydion` |
| 纳入版本管理的 systemd 单元 | `sudo systemctl daemon-reload && sudo systemctl restart <changed-unit>` |
| `.po` 翻译源文件 | `.venv/bin/python tools/compile_translations.py && sudo systemctl reload keydion` |
| `static/` 下的任何内容 | 无需操作，nginx 直接从磁盘提供 |
| `nginx` 配置 | `sudo nginx -t && sudo systemctl reload nginx` |

`reload` 会向 gunicorn master 发送 `SIGHUP`：新工作进程使用更新后的代码启动，旧工作进程处理完进行中的请求后退出。不会丢失连接。

部署后的快速检查：

```bash
sudo systemctl status keydion-publishing-worker --no-pager
sudo systemctl status keydion-attachment-worker --no-pager
sudo systemctl status keydion-paper-integrity.timer --no-pager
sudo systemctl status keydion --no-pager      # active (running)
sudo journalctl -u keydion-publishing-worker -n 30 --no-pager
sudo journalctl -u keydion-attachment-worker -n 30 --no-pager
sudo journalctl -u keydion -n 30 --no-pager   # look for fresh "Booting worker"
sudo -u keydion /Keydion/.venv/bin/python -m tools.publishing_worker --status
sudo -u keydion /Keydion/.venv/bin/python -m tools.verify_alembic_state
curl -sI https://www.keydion.com/ | head -5   # 200/302, Server: nginx
```

如果 journal 中出现的是 traceback 而不是新的工作进程启动日志，说明新代码导入失败，请修复磁盘上的代码后再次 reload。

## Docker 参考栈

`docker-compose.prod.yml` **不是生产环境的权威方案**。生产运维、迁移、工作进程监管和回滚使用纳入版本管理的宿主机 systemd 单元和迁移运维手册。该 Compose 文件运行 Gunicorn、附件工作进程和 nginx；它仍然不能替代为宿主机部署定义的发布工作进程与完整性 timer 运维操作。

对于明确为非生产的参考环境，该栈构建随附的 [`Dockerfile`](Dockerfile)（Python 3.14 + Tesseract），OCR 引擎和中文语言数据已内置其中。

> **未随附 MySQL。** 生产 Compose 需要**外部 MySQL 9.x**（参见[数据库设置](#数据库设置)中的 `VECTOR` 说明）。请将 `.env.prod` 中的 `PAPERQUERY_DATABASE_URL` 指向*从容器内*可达的主机：容器内的 `127.0.0.1` 是容器自身，因此对于宿主机上的 MySQL，请使用 Docker 宿主机的局域网 IP 或 `host.docker.internal`。

1. 完全按照上文**生产部署**一节的方式创建 `.env.prod`（高强度的 `PAPERQUERY_SECRET`、公开的 `PAPERQUERY_MS_REDIRECT_URI`，以及容器可达的 `PAPERQUERY_DATABASE_URL`）。

2. 构建并启动该栈：

   ```bash
   docker compose -f docker-compose.prod.yml up -d --build
   ```

nginx 监听 80 端口；`web` 和 `nginx` 通过共享的 unix socket（`keydion-socket` 卷）通信，nginx 直接从挂载的 `./static` 提供 `/static/*`。TLS 不在范围内：请在前置的 certbot 或负载均衡器上终结 HTTPS，或向 [`docker/nginx.conf`](docker/nginx.conf) 添加 `443` server 块。

`./papers`、`./data` 和 `./static/uploads` 目录以绑定挂载方式接入，因此上传的 PDF 和运行时数据在重新构建后仍保留在宿主机上。执行 `git pull` 后，重新构建并重新创建：

```bash
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml logs -f web   # watch for "Booting worker"
```

## 更新 Python 依赖

`requirements.txt` 包含应用的直接依赖约束。`requirements.lock` 由 `pip-tools` 生成，同时固定直接依赖和传递依赖（含哈希值），以保证可复现安装。请勿手动编辑 `requirements.lock`。请使用生产容器所用的 Python 版本（当前为 Python 3.14）生成该文件。

在虚拟环境中安装一次锁文件工具：

```bash
.venv/bin/python -m pip install pip-tools
```

修改 `requirements.txt` 后，更新其约束允许范围内的所有依赖。`--reuse-hashes` 可以避免为未变化的包版本重新计算哈希：

```bash
.venv/bin/pip-compile \
  --verbose \
  --upgrade \
  --generate-hashes \
  --reuse-hashes \
  --output-file=requirements.lock \
  requirements.txt
```

如果只想更新某一个直接依赖或传递依赖，请将 `--upgrade` 替换为定向升级，例如：

```bash
.venv/bin/pip-compile \
  --verbose \
  --upgrade-package Werkzeug \
  --generate-hashes \
  --reuse-hashes \
  --output-file=requirements.lock \
  requirements.txt
```

如果 `pip-compile` 似乎卡住，用 `Ctrl+C` 中断，并单独测试能否访问包索引：

```bash
.venv/bin/python -m pip index versions Flask --timeout 15 --retries 1
```

如果该命令成功，在限制网络重试次数的同时保持详细进度输出，重新尝试编译：

```bash
PIP_DEFAULT_TIMEOUT=15 PIP_RETRIES=1 \
.venv/bin/pip-compile \
  --verbose \
  --upgrade \
  --generate-hashes \
  --reuse-hashes \
  --output-file=requirements.lock \
  requirements.txt
```

提交前验证生成的锁文件：

```bash
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip check
git diff -- requirements.txt requirements.lock
```

## 用户管理

你可以使用提供的 CLI（命令行界面）工具管理用户（创建、更新、列出）：

```bash
# Create an admin user
python3 tools/manage_passwords.py set --username admin --password MySecurePassword --role 3

# List all users
python3 tools/manage_passwords.py list
```

**角色：**
- `1`：读者（查看与下载）
- `2`：贡献者（可上传）
- `3`：管理员（完全访问）

## 构建搜索索引

在配置 LLM 之前上传的论文不会自动生成 embedding。设置 `LLM_API_KEY`（以及可选的 `LLM_EMBED_*`）后运行一次以下命令，为缺失的论文补建索引：

```bash
python3 tools/build_embeddings.py
```

该脚本默认支持断点续跑，已存有分片的论文会被跳过。如需强制完整重建索引：

```bash
python3 tools/build_embeddings.py --rebuild
```

新论文在索引之前会先持久化发布。内联尝试可能在上传期间完成；否则由独立的发布工作进程负责持久化重试任务。如果某次尝试失败，论文仍保持已发布状态，界面会显示索引警告，后续工作进程的重试可以将其恢复。

## 本地化

项目使用 Flask-Babel 处理翻译。更新翻译的步骤：

1. 将可翻译字符串重新提取到 `messages.pot`：
   ```bash
   python3 tools/extract_translations.py
   ```
   请使用该脚本，而非直接使用 `pybabel extract`。Babel 的默认目录过滤器会跳过所有名称以 `.` 或 `_` 开头的目录，悄无声息地丢掉 `templates/_dashboard/` 等嵌套模板包；该脚本传入的 `--ignore-dirs` 设置保留了这些目录（并排除虚拟环境）。参见 `babel.cfg`。
2. 将新字符串合并到各个翻译目录（保留已有翻译，将新字符串添加为空的 `msgstr`，并将发生变化的源文本标记为 fuzzy）：
   ```bash
   python3 tools/update_translations.py
   ```
3. 在 `translations/*/LC_MESSAGES/messages.po` 中填写新的 `msgstr` 值。
4. 编译翻译：
   ```bash
   python3 tools/compile_translations.py
   ```
   Flask-Babel 在启动时加载 `.mo` 文件，因此编译后需要重启开发服务器，新翻译才会生效。

## 项目结构

- `app.py`：应用工厂核心，包括 `create_app()`、认证/账户/管理路由、模板过滤器和旧版重定向。导入 `app` 不会连接 MySQL。
- `models.py`：SQLAlchemy ORM 模型，以及空数据库引导和 schema 校验。
- `routes/`：按领域划分的 HTTP 路由；每个模块暴露 `register_routes(app)`。
- `services/`：按领域划分的领域逻辑（数据库/存储辅助函数）；绝不导入 `app`。
- `templates/`：Web 界面的 Jinja2 模板（仪表盘面板位于 `templates/_dashboard/`）。
- `static/`：CSS、JavaScript 和图片资源；Bootstrap 和其他第三方库位于 `static/vendor/`。
- `data/`：JSON 配置（论文/新闻/指南分类，EE 和 IA 科目）以及运行时的 `pending_papers/`。
- `papers/`：已上传 PDF 文件的存储目录。
- `translations/`：Flask-Babel 的 `en`/`zh` 翻译目录。
- `migrations/`：Alembic 迁移，所有 schema 变更都通过 Alembic 进行。
- `tests/`：约定测试（基于 `unittest`，结合 AST 解析与 Jinja2 渲染）。
- `tools/`：管理工具脚本（用户管理、翻译、embedding、工作进程）。
- `deploy/`：生产 nginx 配置和 systemd 单元。
- `docs/`：部署运维手册、架构决策记录和 agent 文档。

## 许可证

版权所有 © 2026 Keydion。本项目采用 Apache License 2.0 许可；完整条款见 [LICENSE](LICENSE)。
