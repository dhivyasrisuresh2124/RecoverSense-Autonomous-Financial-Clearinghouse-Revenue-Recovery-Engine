import sys
import os
import unittest
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

stream = StringIO()
runner = unittest.TextTestRunner(stream=stream, verbosity=2)
suite = unittest.TestLoader().loadTestsFromName("backend.tests.test_observability")
result = runner.run(suite)

with open("obs_test_results.txt", "w", encoding="utf-8") as f:
    f.write(stream.getvalue())
    f.write(f"\nTests run: {result.testsRun}\n")
    f.write(f"Failures: {len(result.failures)}\n")
    f.write(f"Errors: {len(result.errors)}\n")
    if result.wasSuccessful():
        f.write("SUCCESS\n")
    else:
        f.write("FAILED\n")

print(f"Tests run: {result.testsRun}")
print(f"Failures: {len(result.failures)}")
print(f"Errors: {len(result.errors)}")
if result.wasSuccessful():
    print("SUCCESS")
else:
    print("FAILED")
