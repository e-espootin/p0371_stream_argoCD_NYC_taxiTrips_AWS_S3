

## build image
docker build -t taxitrips-stream-collector .
docker run -it --rm --network host taxitrips-stream-collector:latest
# build and push docker hub
docker build -t e4espootin/taxitrips-stream-collector .
docker push e4espootin/taxitrips-stream-collector



# docker run
docker run -it --rm -d --network host --name taxitrips-stream-collector e4espootin/taxitrips-stream-collector 
# python run
python main.py 

# verify connection
docker exec -it container1 ping container2

## pull
docker pull e4espootin/taxitrips-stream-collector:latest
docker run -it --rm --network host --name taxitrips-stream-collector e4espootin/taxitrips-stream-collector:latest 