# Rental Analyzer

Desktop application for tracking short-term rental prices across booking sites.

## Development

Run the app:

```powershell
python main.py
```

Run tests:

```powershell
.\venv\Scripts\python.exe -m pytest -q
```

Clean generated artifacts without touching app data:

```powershell
.\scripts\clean-workspace.ps1
```

Clean generated artifacts and local runtime data:

```powershell
.\scripts\clean-workspace.ps1 -IncludeRuntimeData
```

## Project layout

- `app/` - application source code
- `tests/` - automated tests
- `data/` - local runtime data
- `logs/` - runtime logs
- `build.spec` - PyInstaller build config
