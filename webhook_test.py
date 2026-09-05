import os, hmac, hashlib
body = b'{"event":"payment.failed","payload":{"payment":{"entity":{"id":"pay_TX32eRjRnEwzeb","amount":10000,"currency":"INR","status":"failed","error_code":"BAD_REQUEST_ERROR","error_description":"Test failure for RecoverSense"}}}}'
sig = hmac.new(
    os.environ["RAZORPAY_WEBHOOK_SECRET"].encode(),
    body,
    hashlib.sha256
).hexdigest()
open("webhook_test_body.json", "wb").write(body)
open("webhook_test_signature.txt", "w").write(sig)
print("Signature generated: YES")
print("Signature length:", len(sig))
