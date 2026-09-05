import sys, base64
with open(sys.argv[1], sys.argv[2], encoding='utf-8') as f:
    f.write(base64.b64decode(sys.argv[3]).decode('utf-8'))
