# Keydion

sudo apt update
  sudo apt install -y python3 python3-venv python3-pip build-essential
  cd /var/Keydion
  python3 -m venv .venv
  . .venv/bin/activate
  pip install -r requirements.txt
  ./run.sh


## 常用环境变量

| 变量名 | 作用 | 默认值 |
|--------|------|--------|
| `PAPERQUERY_SECRET` | Flask Secret Key | `dev-secret-key` |
| `PAPERQUERY_DATA_DIR` | 存放 CSV 等数据目录 | `<项目根>/data` |
| `PAPERQUERY_USERS_CSV` | 用户信息 CSV 路径 | `<数据目录>/users.csv` |
| `PAPERQUERY_UPLOAD_DIR` | 论文 PDF 存储目录 | `<项目根>/papers` |
| `PAPERQUERY_PBKDF_ITERATIONS` | PBKDF2 迭代次数 | `260000` |
| `PAPERQUERY_SESSION_TIMEOUT` | 单账号会话保持时长（秒） | `600` |
| `PAPERQUERY_MS_CLIENT_ID` | Microsoft 应用 Client ID | 空 |
| `PAPERQUERY_MS_CLIENT_SECRET` | Microsoft 应用 Client Secret | 空 |
| `PAPERQUERY_MS_REDIRECT_URI` | Microsoft 登录回调地址 | `http://127.0.0.1:5000/auth/callback` |
| `PAPERQUERY_MS_AUTHORITY` | Microsoft 登录授权地址 | `https://login.microsoftonline.com/common` |

## tools 工具脚本

- `manage_passwords.py`：用于旧版 CSV 登录的用户哈希管理（使用 Microsoft 登录时可忽略）。
  ```bash
  python tools/manage_passwords.py set --username alice --password Secret123 --role 2 --registration-date 2024-09-01 --expiry-date 2025-09-01
  python tools/manage_passwords.py list
  ```
- `compile_translations.py`：使用 Babel 将 `translations/*/LC_MESSAGES/messages.po` 编译为 `.mo` 文件。
  ```bash
  python tools/compile_translations.py
  ```


