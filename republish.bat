@echo off
cd /d "%~dp0"
echo Rebuilding features...
python -m harness_model.cli build-features --db data/harness.db --csv data/features/runner_features.csv --track-pars data/track_pars.json
echo.
echo Republishing all meetings and live site...
python -m harness_model.cli republish-all --db data/harness.db --csv data/features/runner_features.csv --weights weights.json
echo.
pause
