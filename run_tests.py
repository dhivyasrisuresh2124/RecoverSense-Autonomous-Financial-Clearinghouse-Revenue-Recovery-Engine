import sys
import io
import unittest

output = io.StringIO()
runner = unittest.TextTestRunner(stream=output, verbosity=2)
suite = unittest.defaultTestLoader.loadTestsFromName("backend.tests.test_observability")
result = runner.run(suite)

with open("test_output.txt", "w", encoding="utf-8") as f:
    f.write(output.getvalue())
    f.write(f"\nTests run: {result.testsRun}, Failures: {len(result.failures)}, Errors: {len(result.errors)}\n")

sys.exit(0 if result.wasSuccessful() else 1)
