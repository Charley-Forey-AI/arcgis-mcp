# ArcGIS API for Python

This project uses the **ArcGIS API for Python** in a dedicated Python 3.12 virtual environment (the API supports Python 3.10–3.13 only; Python 3.14 is not supported).

## Activate the environment

Before running ArcGIS scripts or Jupyter, activate the venv:

**PowerShell:**
```powershell
.\.venv\Scripts\Activate.ps1
```

**Command Prompt (cmd):**
```cmd
.venv\Scripts\activate.bat
```

When active, your prompt will show `(.venv)`.

## Run scripts

1. Activate the venv (see above).
2. Run your script:
   ```powershell
   python your_script.py
   ```

## Use Jupyter Lab

1. Activate the venv.
2. Start Jupyter Lab:
   ```powershell
   jupyter lab
   ```
3. Create or open a notebook; the kernel will use the venv and the ArcGIS API (including the map widget).

## Installed packages

- **arcgis** (2.4.2) – ArcGIS API for Python
- **arcgis-mapping** (4.33.1) – Interactive 2D maps and 3D scenes

To reinstall or update:
```powershell
.\.venv\Scripts\Activate.ps1
pip install arcgis arcgis-mapping
```
