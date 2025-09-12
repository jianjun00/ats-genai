# 🚀 ATS Autostart Setup Instructions

This guide will set up ATS-DEV analytics service to start automatically with WSL.

## ✅ **What Was Fixed**

1. **✅ Added ATS-DEV Analytics to autostart script** - Now starts both PostgreSQL and Analytics
2. **✅ Fixed analytics service entry point** - Using `unified_analytics_service.py` instead of non-existent `analytics_service.py`
3. **✅ Docker credential issues resolved** - Modern Docker config in place
4. **✅ Created systemd service setup script**

## 🔧 **Manual Setup Required (One-time)**

Run these commands to enable automatic startup:

```bash
# 1. Install and enable systemd service (requires sudo)
./scripts/setup_autostart.sh

# 2. Verify service is installed
systemctl status ats-autostart

# 3. Test autostart manually (optional)
sudo systemctl start ats-autostart
```

## 🎯 **What Will Start Automatically**

After WSL boots, the following services will start automatically:

- **✅ ATS-DEV PostgreSQL**: `localhost:3432` (dev_db)
- **✅ ATS-DEV Analytics**: `http://localhost:3000` (unified analytics service)
- **✅ ATS-INTG PostgreSQL**: `localhost:4432` (intg_db)
- **✅ ATS-INTG Analytics**: `http://localhost:4000`
- **✅ ATS-INTG Services**: Minute bars, monitoring, Slack notifications

## 📊 **Verification Commands**

Check that everything is working:

```bash
# Check service status
systemctl status ats-autostart

# Check running containers
python3 scripts/run_dev.py status
docker ps | grep ats

# Test service endpoints
curl http://localhost:3000/health  # ATS-DEV Analytics
curl http://localhost:4000/health  # ATS-INTG Analytics

# View autostart logs
tail -20 /mnt/d/ats-logs/autostart.log
```

## 🛠️ **Current Status**

- ✅ **ATS-DEV PostgreSQL**: Running (Up 32 minutes)
- ✅ **ATS-DEV Analytics**: Running and healthy at http://localhost:3000
- ✅ **ATS-INTG Services**: All running
- ❌ **Systemd Service**: Not yet installed (requires manual sudo setup)

## 🆘 **If Issues Occur**

**Service won't start:**
```bash
# Check logs
journalctl -u ats-autostart -f

# Manual restart
sudo systemctl restart ats-autostart
```

**Analytics service fails:**
```bash
# Check container logs
docker logs ats-dev-analytics

# Manual restart
python3 scripts/run_dev.py stop --service analytics
python3 scripts/run_dev.py start --service analytics
```

## 📋 **Service URLs After Setup**

- **ATS-DEV Analytics**: http://localhost:3000/health
- **ATS-DEV Database**: postgresql://postgres:dev_password@localhost:3432/dev_db
- **ATS-INTG Analytics**: http://localhost:4000/health
- **ATS-INTG Database**: postgresql://postgres:intg_password@localhost:4432/intg_db

---

**✅ Ready to go! Run `./scripts/setup_autostart.sh` to complete the setup.**