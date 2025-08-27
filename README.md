# Redis + MongoDB Hybrid Storage System

一个支持Redis和MongoDB混合存储的智能存储系统，具有自动内存压力检测、智能数据迁移和无缝CRUD操作功能。

## ⚠️ 重要要求

**本地必须安装并运行Redis服务，且Redis必须无密码配置才能正常运行！**

### Redis安装和配置

#### 1. 安装Redis

**macOS (使用Homebrew):**
```bash
brew install redis
```

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install redis-server
```

**CentOS/RHEL:**
```bash
sudo yum install redis
```

#### 2. 启动Redis服务

**macOS:**
```bash
brew services start redis
```

**Linux:**
```bash
sudo systemctl start redis
sudo systemctl enable redis
```

#### 3. 验证Redis运行状态

```bash
redis-cli ping
```

应该返回 `PONG`

#### 4. 检查Redis配置

确保Redis配置文件中没有设置密码：

```bash
# 查看Redis配置文件位置
redis-cli config get dir

# 编辑配置文件（通常是 /etc/redis/redis.conf 或 /usr/local/etc/redis.conf）
# 确保以下行被注释掉或删除：
# requirepass your_password
```

#### 5. 重启Redis服务

修改配置后重启Redis：

**macOS:**
```bash
brew services restart redis
```

**Linux:**
```bash
sudo systemctl restart redis
```
