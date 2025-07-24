Manual CLI debugging for universe_state_manager CLI integration test:

To debug the CLI manually, run the following commands from the project root:

# 1. Set up environment variables
export PYTHONPATH=$(pwd)/src:$(pwd)/intg_tests

# 2. Write DummyBuilder to a temp directory (or use the one in the test)
# 3. Run build action:
python3 src/state/universe_state_manager.py \
  --start_date 2025-01-01 --end_date 2025-07-18 \
  --universe_id test_universe --action build \
  --saved_dir /tmp/cli_test_states

# 4. Run inspect action for AAPL or TSLA on a sample date:
python3 src/state/universe_state_manager.py \
  --start_date 2025-01-02 --end_date 2025-01-02 \
  --universe_id test_universe --action inspect \
  --instrument_id AAPL --mode print \
  --fields low high close pldot oneonedot etop ebot \
  --saved_dir /tmp/cli_test_states

# 5. Check output for expected signals.

If you see errors, check the DummyBuilder import path and PYTHONPATH. If the CLI runs but does not print expected signals, check the state file contents in /tmp/cli_test_states/states/.
