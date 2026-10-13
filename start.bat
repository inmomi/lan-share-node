@echo off
chcp 65001 >nul
cd /d "%~dp0"

if "%PORT%"=="" set PORT=8080
if "%UPLOAD_DIR%"=="" set UPLOAD_DIR=%~dp0uploads

echo Starting LAN Share on port %PORT% ...
echo Upload folder: %UPLOAD_DIR%
if not "%LAN_SHARE_TURN%"=="" echo TURN relay    : %LAN_SHARE_TURN%
echo.

rem 需要 Node >= 18。原来这里写死了一台机器上的 node.exe 绝对路径，换台电脑
rem 或者别人 clone 下来就直接跑不起来；改成优先 PATH，其次 npx 拉临时运行时。
where node >nul 2>nul
if %errorlevel%==0 (
  node server.js
  goto done
)

where npx >nul 2>nul
if %errorlevel%==0 (
  echo node 不在 PATH 里，改用 npx 拉取临时运行时...
  npx --yes node@22 server.js
  goto done
)

echo.
echo [错误] 没找到 Node.js。请先安装 Node 18 或更高版本： https://nodejs.org/
echo        安装后重新打开一个命令行窗口，再双击本脚本。

:done
echo.
pause
