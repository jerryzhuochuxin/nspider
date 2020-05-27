docker stop nspider-1
docker rm nspider-1
docker rmi nspider
docker build -t nspider .
docker run -itd --name nspider-1 nspider
