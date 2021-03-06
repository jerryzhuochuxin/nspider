pip3 install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# mongodb://localhost:27017/biliob
# redis://localhost:6379
export BILIOB_REDIS_CONNECTION_STRING="redis://:123456@localhost:6379"
export BILIOB_MONGO_URL="mongodb://user:123456@localhost:27017/biliob?authMechanism=SCRAM-SHA-256"

python3 main.py -debug=False
