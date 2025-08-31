# Moving WSL2 to D: Drive - Complete Guide

## 🎯 **Why Move WSL to D: Drive**
- **C: Drive**: 922GB used / 930GB total (100% full) - causing crashes
- **D: Drive**: 867GB used / 4.6TB total (19% used) - plenty of space
- **Benefits**: Prevent WSL crashes, better performance, more room for development

## 🚀 **Method 1: WSL Export/Import (Recommended)**

### **Step 1: Export Current WSL Distribution**
```powershell
# Run in Windows PowerShell as Administrator
# Check current WSL distributions
wsl --list --verbose

# Export your current Ubuntu distribution
wsl --export Ubuntu D:\WSL-Backup\ubuntu-backup.tar

# This will create a backup of your entire WSL environment
```

### **Step 2: Unregister Current WSL**
```powershell
# Stop WSL
wsl --shutdown

# Unregister (this deletes the current installation)
wsl --unregister Ubuntu
```

### **Step 3: Import to D: Drive**
```powershell
# Create directory for WSL on D: drive
mkdir D:\WSL-Distributions

# Import WSL to D: drive
wsl --import Ubuntu D:\WSL-Distributions\Ubuntu D:\WSL-Backup\ubuntu-backup.tar
```

### **Step 4: Set Default User (Important!)**
```powershell
# Start WSL and set your user as default
wsl -d Ubuntu

# Inside WSL, run:
echo "[user]" | sudo tee /etc/wsl.conf
echo "default=jianjun" | sudo tee -a /etc/wsl.conf

# Exit WSL and restart
exit
wsl --shutdown
wsl -d Ubuntu
```

## 🔧 **Method 2: WSL Configuration File**

### **Create WSL Global Config on D: Drive**
```powershell
# Create .wslconfig in user directory
notepad C:\Users\%USERNAME%\.wslconfig
```

Add this content:
```ini
[wsl2]
# Move WSL2 virtual hard disk to D: drive
# Note: This only works for new installations
```

## 🛠️ **Method 3: Manual VHDX Move**

### **Find Current WSL Virtual Disk**
```powershell
# WSL virtual disks are usually located at:
# C:\Users\%USERNAME%\AppData\Local\Packages\CanonicalGroupLimited.Ubuntu*\LocalState\ext4.vhdx

# Find the exact location
Get-ChildItem -Path "C:\Users\$env:USERNAME\AppData\Local\Packages" -Recurse -Name "ext4.vhdx"
```

### **Move VHDX to D: Drive**
```powershell
# 1. Stop WSL completely
wsl --shutdown

# 2. Create directory on D: drive
mkdir D:\WSL-Storage

# 3. Move the VHDX file (replace path with actual location)
move "C:\Users\jianjun\AppData\Local\Packages\CanonicalGroupLimited.Ubuntu*\LocalState\ext4.vhdx" "D:\WSL-Storage\"

# 4. Create symbolic link back to original location
mklink "C:\Users\jianjun\AppData\Local\Packages\CanonicalGroupLimited.Ubuntu*\LocalState\ext4.vhdx" "D:\WSL-Storage\ext4.vhdx"
```

## ✅ **Verification Steps**

### **Check WSL Location**
```powershell
# List WSL distributions and their locations
wsl --list --verbose

# Check disk usage
wsl -e df -h
```

### **Test WSL Services**
```bash
# Inside WSL, verify everything works:
docker ps
cd /home/jianjun/ats-genai-admin
./scripts/ats_startup.sh
```

## 🎯 **Expected Results**

**Before Move:**
- WSL on C: drive (100% full)
- Frequent WSL crashes
- Limited development space

**After Move:**
- WSL on D: drive (4.6TB available)
- Stable WSL operation
- Room for expansion

## ⚠️ **Important Notes**

1. **Backup First**: Always backup your WSL environment before moving
2. **Docker Data**: Docker volumes will remain, but containers will need restart
3. **File Paths**: Some absolute paths may need updating
4. **Performance**: D: drive (HDD) may be slower than C: drive (SSD)

## 🚨 **Risk Mitigation**

**Before Starting:**
```bash
# Backup critical ATS data
docker exec ats-dev-postgres pg_dump -U postgres dev_db > /mnt/d/ats-backup/pre-move-backup.sql
docker exec ats-intg-postgres pg_dump -U postgres intg_db > /mnt/d/ats-backup/intg-pre-move-backup.sql

# Backup WSL environment
tar -czf /mnt/d/ats-backup/wsl-home-backup.tar.gz /home/jianjun/
```

**Recovery Plan:**
If anything goes wrong, you can restore from the export file created in Step 1.

## 🎉 **Benefits After Move**

- ✅ **No more WSL crashes** due to disk space
- ✅ **4.6TB available** for development and data
- ✅ **Stable FirstRate processing** without I/O bottlenecks
- ✅ **Room for Docker volumes** and large datasets
- ✅ **Better system performance** overall

---

**Recommendation: Use Method 1 (Export/Import) as it's the safest and most reliable approach.**