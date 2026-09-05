import sys
import traceback

try:
    with open("test_debug.txt", "w") as f:
        f.write("Starting test\n")
        f.flush()
        
        import unittest
        output = unittest.main(
            module="backend.tests.test_observability",
            argv=[""],
            exit=False,
            verbosity=2,
        )
        f.write(f"Tests run: {output.testsRun}\n")
        f.write(f"Failures: {len(output.failures)}\n")
        f.write(f"Errors: {len(output.errors)}\n")
        f.flush()
        
except Exception as e:
    with open("test_debug.txt", "a") as f:
        f.write(f"ERROR: {traceback.format_exc()}\n")
        f.flush()
    print(f"ERROR: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
