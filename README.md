# PaperQuery

PaperQuery 是一个面向国际学校的轻量级论文查询门户，为不同权限的学生与教师提供论文检索、上传与管理能力，并支持中英双语界面切换。

## 功能亮点

- **CSV 用户认证**：按权限区分“只读 / 上传 / 管理”三个等级，密码使用 PBKDF2 哈希存储。
- **单账号互斥登录**：同一账号仅允许 1 个活跃会话；若未正常退出，在 10 分钟无操作后自动释放，可通过环境变量调节时长。
- **关键词检索**：支持按标题与正文内容检索已上传的 PDF 论文。
- **PDF 上传与删除**：贡献者可上传，管理员可删除；系统阻止重名覆盖并要求二次确认。
- **现代化界面**：基于 Bootstrap 5 的定制设计，搭配渐变与动效增强视觉层次。
- **中英双语**：导航栏内置语言切换菜单，可即时在英文与中文之间切换。

## 快速开始

1. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```
2. **配置用户**
   编辑 `data/users.csv` 调整门户账号，字段说明：
   - `username`：用户名
   - `password`：PBKDF2 哈希（使用 `tools/manage_passwords.py` 生成）
   - `registration_date`：注册日期，格式 `YYYY-MM-DD`
   - `expiry_date`：过期日期，留空表示不过期
   - `role`：权限等级（1=只读，2=上传，3=删除）
3. **准备论文**
   将 PDF 文件放入 `papers/` 文件夹，或通过上传界面提交。系统使用 [PyPDF2](https://pypi.org/project/PyPDF2/) 解析内容以便检索。
4. **启动服务**
   - Windows：
     ```powershell
     .\start_local.ps1    # 可选参数：-Secret / -DataDir / -UploadDir / -Venv
     ```
   - macOS / Linux：
     ```bash
     chmod +x start_local.sh
     ./start_local.sh     # 可通过 PAPERQUERY_* 或 VENV_PATH 覆盖默认值
     ```
   启动后访问 `http://127.0.0.1:5000/`。

## 默认账户

| 用户名        | 密码         | 权限说明        |
|---------------|--------------|-----------------|
| `reader`      | `reader123`  | 只读访问        |
| `contributor` | `upload123`  | 可上传论文      |
| `admin`       | `admin123`   | 可上传与删除论文 |

> 提示：密码使用 PBKDF2 哈希存储；生产环境请启用 HTTPS，并考虑接入更完善的认证体系。

## 常用环境变量

| 变量名 | 作用 | 默认值 |
|--------|------|--------|
| `PAPERQUERY_SECRET` | Flask Secret Key | `dev-secret-key` |
| `PAPERQUERY_DATA_DIR` | 存放 CSV 等数据文件的目录 | `<项目根>/data` |
| `PAPERQUERY_USERS_CSV` | 用户信息 CSV 文件路径 | `<数据目录>/users.csv` |
| `PAPERQUERY_UPLOAD_DIR` | 论文 PDF 存储目录 | `<项目根>/papers` |
| `PAPERQUERY_PBKDF_ITERATIONS` | PBKDF2 迭代次数 | `260000` |
| `PAPERQUERY_SESSION_TIMEOUT` | 单账号会话保持时长（秒），默认 600 | `600` |

## tools 目录

- `manage_passwords.py`：新增或更新用户，并生成 PBKDF2 哈希。
  ```bash
  python tools/manage_passwords.py set --username alice --password Secret123 --role 2 --registration-date 2024-09-01 --expiry-date 2025-09-01
  python tools/manage_passwords.py list
  ```
- `compile_translations.py`：使用 Babel 将 `translations/*/LC_MESSAGES/messages.po` 编译为 `.mo` 文件。
  ```bash
  python tools/compile_translations.py
  ```

## 更新翻译

1. 在 `translations/zh/LC_MESSAGES/messages.po` 或 `translations/en/LC_MESSAGES/messages.po` 中编辑文案。
2. 运行 `python tools/compile_translations.py` 重新生成 `.mo` 文件。
3. 重启应用或刷新页面，即可看到语言更新。导航栏右侧可即时切换中文 / 英文，无需重复修改模板。

## 本地测试建议

1. 安装依赖并准备若干示例 PDF（放入 `papers/`）。
2. 启动应用后，使用默认或自建账户登录，逐项验证“检索 → 上传 → 下载 → 删除”流程。
3. 在导航栏切换语言，确认双语内容与提示信息是否正确。
4. 若新增账户，先执行 `tools/manage_passwords.py set ...` 生成哈希，再登录验证。

## 后续扩展方向

- 引入数据库或目录服务，记录操作日志并提升账号管理能力。
- 为论文加入标签、元数据与内容摘要，增强检索体验。
- 接入对象存储（如 S3）或全文搜索服务，以应对更大规模的论文库。
