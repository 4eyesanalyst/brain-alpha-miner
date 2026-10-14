' Starts the BRAIN alpha miner in the background (no window). Safe to run twice: the miner exits if already running.
Set shell = CreateObject("WScript.Shell")
shell.CurrentDirectory = "C:\Users\USER\Downloads\Quant"
shell.Run """" & shell.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\Programs\Python\Python312\pythonw.exe"" ""C:\Users\USER\Downloads\Quant\miner.py""", 0, False
