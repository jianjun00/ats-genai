# Fast WSL Migration to D: Drive - Speed Optimized Methods

## 🚀 **Fastest Methods (Skip Full Export)**

### **Method 1: Direct VHDX Move (Fastest - 5-10 minutes)**

```powershell
# Run in Windows PowerShell as Administrator

# 1. Stop WSL completely
wsl --shutdown

# 2. Find your WSL VHDX file location
$wslPath = Get-ChildItem -Path "$env:USERPROFILE\AppData\Local\Packages" -Recurse -Name "ext4.vhdx" | ForEach-Object { 
    Join-Path (Get-ChildItem -Path "$env:USERPROFILE\AppData\Local\Packages" -Directory | Where-Object { $_.Name -like "*Ubuntu*" }).FullName "LocalState\ext4.vhdx"
}
Write-Host "Found WSL at: $wslPath"

# 3. Create D: drive WSL directory
mkdir D:\WSL-Storage -Force

# 4. Move VHDX file (this is just a file move - very fast!)
move "$wslPath" "D:\WSL-Storage\ext4.vhdx"

# 5. Create symbolic link back to original location
New-Item -ItemType SymbolicLink -Path "$wslPath" -Target "D:\WSL-Storage\ext4.vhdx"

# 6. Start WSL
wsl
```

### **Method 2: Use Existing Backup (2-3 minutes)**
You already have a 366GB backup on D: drive!

```powershell
# 1. Stop current WSL
wsl --shutdown

# 2. Unregister current WSL (fast)
wsl --unregister Ubuntu

# 3. Use existing backup (no export needed!)
wsl --import Ubuntu D:\WSL-Distributions\Ubuntu D:\wsl\Ubuntu.tar

# 4. Set default user
wsl -d Ubuntu
echo -e "[user]\ndefault=jianjun" | sudo tee /etc/wsl.conf
exit
wsl --shutdown
wsl -d Ubuntu
```

### **Method 3: Robocopy for Speed (10-15 minutes)**

```powershell
# 1. Stop WSL
wsl --shutdown

# 2. Find WSL location
$sourcePath = (Get-ChildItem -Path "$env:USERPROFILE\AppData\Local\Packages" -Directory | Where-Object { $_.Name -like "*Ubuntu*" }).FullName + "\LocalState"

# 3. Use Robocopy (much faster than tar)
robocopy "$sourcePath" "D:\WSL-Storage\LocalState" /E /COPY:DAT /R:1 /W:1 /MT:16

# 4. Move original and create link
move "$sourcePath\ext4.vhdx" "$sourcePath\ext4.vhdx.backup"
New-Item -ItemType SymbolicLink -Path "$sourcePath\ext4.vhdx" -Target "D:\WSL-Storage\LocalState\ext4.vhdx"
```

## ⚡ **Speed Optimization Tips**

### **Why Export is Slow:**
- Creates compressed tar archive (CPU intensive)
- Single-threaded operation
- Processes entire 366GB+ filesystem
- C: drive is 100% full (disk I/O bottleneck)

### **Speed Improvements:**
1. **Skip compression**: Use direct file operations
2. **Multi-threaded copy**: Use `robocopy /MT:16`
3. **Avoid full backup**: Use existing backup or direct move
4. **SSD optimization**: Use `fsutil behavior set DisableDeleteNotify 0`

## 🎯 **Recommended: Method 1 (Direct VHDX Move)**

**Advantages:**
- ✅ **Fastest**: Just moves a single file
- ✅ **Preserves everything**: All data, containers, configs
- ✅ **Reversible**: Can undo with another move
- ✅ **No downtime**: Minimal WSL offline time

**Time Estimate:**
- VHDX move: 2-5 minutes (depending on file size)
- Symbolic link: Instant
- **Total: 5-10 minutes max**

## 🛡️ **Safety Measures**

### **Before Starting:**
```bash
# Quick backup of critical ATS data (runs in background)
nohup docker exec ats-dev-postgres pg_dump -U postgres dev_db > /mnt/d/ats-backup/emergency-backup-$(date +%Y%m%d).sql &
nohup docker exec ats-intg-postgres pg_dump -U postgres intg_db > /mnt/d/ats-backup/intg-emergency-backup-$(date +%Y%m%d).sql &

# Backup docker volumes list
docker volume ls > /mnt/d/ats-backup/docker-volumes-$(date +%Y%m%d).txt
```

### **Recovery Plan:**
```powershell
# If anything goes wrong, reverse the move:
wsl --shutdown
remove-item "$wslPath"  # Remove symbolic link
move "D:\WSL-Storage\ext4.vhdx" "$wslPath"  # Move back
wsl
```

## 📊 **Performance Comparison**

| Method | Time | Risk | Complexity |
|--------|------|------|------------|
| Full Export | 2-4 hours | Low | Low |
| Direct VHDX Move | 5-10 min | Medium | Medium |
| Use Existing Backup | 2-3 min | Low | Low |
| Robocopy | 10-15 min | Medium | High |

## 🎉 **Expected Results**

**After Migration:**
- WSL runs from D: drive (3.8TB free space)
- No more crashes due to disk space
- All Docker containers and data preserved
- ATS services continue working normally
- FirstRate processing can run without I/O conflicts

---

**💡 RECOMMENDATION: Try Method 2 first (use existing backup) - it's fastest and safest. If that backup is too old, use Method 1 (direct VHDX move).**