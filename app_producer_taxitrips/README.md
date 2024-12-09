
## kafka
dev/docker-compose/kafka

## build image
docker build -t taxitrips-stream-producer .
# build and push docker hub
docker build -t e4espootin/taxitrips-stream-producer .
docker push e4espootin/taxitrips-stream-producer



# docker run
docker run -it --rm -d --network host --name taxitrips-stream-producer e4espootin/taxitrips-stream-producer 
# python run
python main.py --broker 192.168.0.108 --port 9092

# verify connection
docker exec -it container1 ping container2

## pull
docker pull e4espootin/taxitrips-stream-producer:latest
docker run -it --rm --network host e4espootin/taxitrips-stream-producer:latest 