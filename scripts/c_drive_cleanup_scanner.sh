#!/bin/bash
# C: Drive Cleanup Scanner
# Identifies large files and directories that can be safely cleaned

echo "🔍 Scanning C: drive for cleanup opportunities..."
echo "This may take several minutes..."

echo ""
echo "=== LARGE FILES SCAN ==="

# Find large files (>100MB) that are commonly safe to delete
echo "🗂️ Large temporary and cache files:"
find /mnt/c -type f \( -name "*.tmp" -o -name "*.temp" -o -name "*.log" -o -name "*.dmp" -o -name "*.old" -o -name "*.bak" \) -size +100M 2>/dev/null | head -20

echo ""
echo "📦 Large installer files:"
find /mnt/c -type f \( -name "*.msi" -o -name "*.exe" \) -path "*/Downloads/*" -size +100M 2>/dev/null | head -10

echo ""
echo "🎬 Large media files in common locations:"
find /mnt/c -type f \( -name "*.mp4" -o -name "*.avi" -o -name "*.mkv" -o -name "*.mov" \) -size +1G 2>/dev/null | head -10

echo ""
echo "=== DIRECTORY SIZE SCAN ==="

# Check common large directories
echo "📁 Windows Update cache:"
du -sh /mnt/c/Windows/SoftwareDistribution 2>/dev/null || echo "Cannot access - may need Windows cleanup"

echo ""
echo "📁 Windows Installer cache:"
du -sh /mnt/c/Windows/Installer 2>/dev/null || echo "Cannot access - may need Windows cleanup"

echo ""
echo "📁 Recycle Bin:"
du -sh /mnt/c/'$Recycle.Bin' 2>/dev/null || echo "Cannot access - may contain deleted files"

echo ""
echo "📁 Temporary Internet Files (if accessible):"
find /mnt/c -path "*/Temporary Internet Files*" -type d -exec du -sh {} \; 2>/dev/null | head -5

echo ""
echo "📁 AppData Local Temp directories:"
find /mnt/c -path "*/AppData/Local/Temp" -type d -exec du -sh {} \; 2>/dev/null | head -10

echo ""
echo "📁 Node.js node_modules (often very large):"
find /mnt/c -name "node_modules" -type d -exec du -sh {} \; 2>/dev/null | sort -hr | head -10

echo ""
echo "📁 Docker Desktop data (if installed):"
find /mnt/c -path "*Docker*" -name "*.vhdx" -exec du -sh {} \; 2>/dev/null
find /mnt/c -path "*Docker Desktop*" -type d -exec du -sh {} \; 2>/dev/null | head -5

echo ""
echo "📁 Visual Studio / Development caches:"
find /mnt/c -path "*/.nuget*" -o -path "*/packages*" -o -path "*/.vs*" | head -10
find /mnt/c -name ".nuget" -type d -exec du -sh {} \; 2>/dev/null | head -5

echo ""
echo "=== SYSTEM FILES SCAN ==="
echo "💾 System files that might be large:"
echo "Hibernation file (hiberfil.sys):"
ls -lh /mnt/c/hiberfil.sys 2>/dev/null || echo "Not accessible"

echo "Page file (pagefile.sys):"  
ls -lh /mnt/c/pagefile.sys 2>/dev/null || echo "Not accessible"

echo "Swap file (swapfile.sys):"
ls -lh /mnt/c/swapfile.sys 2>/dev/null || echo "Not accessible"

echo ""
echo "=== RECOMMENDATIONS ==="
echo "🎯 To free up C: drive space:"
echo "1. Run Windows Disk Cleanup (cleanmgr.exe) as Administrator"
echo "2. Clear Windows Update cache: Delete C:\\Windows\\SoftwareDistribution\\Download\\*"
echo "3. Clear Temp files: Delete C:\\Windows\\Temp\\* and %USERPROFILE%\\AppData\\Local\\Temp\\*"
echo "4. Empty Recycle Bin completely"
echo "5. Uninstall unused programs via Control Panel"
echo "6. Move large files to D: drive (has 3.8TB free space)"
echo "7. Consider disabling hibernation if not needed: powercfg /hibernate off"

echo ""
echo "⚠️ CRITICAL: C: drive at 100% capacity is causing WSL crashes!"
echo "Priority: Free at least 10-20GB immediately for system stability"

echo ""
echo "✅ Scan completed!"