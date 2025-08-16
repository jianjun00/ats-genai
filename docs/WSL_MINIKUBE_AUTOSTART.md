# Setting Up Minikube to Start Automatically in WSL

This guide provides instructions for configuring Minikube to start automatically when your WSL instance launches.

## Method 1: Using Windows Task Scheduler (Recommended)

This method uses Windows Task Scheduler to start Minikube when WSL launches.

### Step 1: Create a Windows Batch File

1. Create a file named `start_minikube_wsl.bat` on your Windows system (e.g., in your Documents folder)
2. Add the following content:

```batch
@echo off
wsl -d Ubuntu -u jianjun /home/jianjun/ats-genai/scripts/start_minikube_wsl.sh
```

**Note:** Replace `Ubuntu` with your WSL distribution name if different, and adjust the username and path as needed.

### Step 2: Create a Task in Windows Task Scheduler

1. Open Task Scheduler (search for it in the Start menu)
2. Click "Create Basic Task..."
3. Name it "Start Minikube in WSL" and click Next
4. Select "When I log on" as the trigger and click Next
5. Select "Start a program" and click Next
6. Browse to the batch file you created and click Next
7. Check "Open the Properties dialog..." and click Finish
8. In the Properties dialog:
   - Go to the Conditions tab
   - Uncheck "Start the task only if the computer is on AC power"
   - Click OK

## Method 2: Using WSL Startup Commands

### Step 1: Add to .profile

Add the Minikube startup script to your `.profile` file to run when you log in:

```bash
# Edit your .profile file
echo '# Start Minikube on WSL startup' >> ~/.profile
echo 'if [ -f "/home/jianjun/ats-genai/scripts/start_minikube_wsl.sh" ]; then' >> ~/.profile
echo '    /home/jianjun/ats-genai/scripts/start_minikube_wsl.sh &' >> ~/.profile
echo 'fi' >> ~/.profile
```

### Step 2: Create a wsl.conf File (Optional)

This step configures WSL to run commands at startup:

```bash
# Create or edit /etc/wsl.conf (requires sudo)
sudo bash -c 'cat > /etc/wsl.conf << EOL
[boot]
command="/home/jianjun/ats-genai/scripts/start_minikube_wsl.sh"
EOL'
```

**Note:** After modifying wsl.conf, you need to restart your WSL instance with `wsl --shutdown` from PowerShell/CMD.

## Method 3: Using Windows Startup Folder

1. Create a shortcut to the batch file you created in Method 1
2. Press Win+R, type `shell:startup` and press Enter
3. Move the shortcut to this folder

## Testing Your Setup

To test if your setup works:

1. From PowerShell or CMD, run: `wsl --shutdown`
2. Restart your WSL terminal
3. Run `minikube status` to check if Minikube started automatically

## Troubleshooting

If Minikube doesn't start automatically:

1. Check if the startup script runs correctly manually:
   ```bash
   /home/jianjun/ats-genai/scripts/start_minikube_wsl.sh
   ```

2. Verify that Minikube can start normally:
   ```bash
   minikube start
   minikube status
   ```

3. Check Windows Task Scheduler history to see if the task ran successfully

4. Ensure your WSL instance has enough resources allocated in `.wslconfig`
