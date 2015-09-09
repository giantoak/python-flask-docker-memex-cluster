# Flask Dockerized Application to analyze clusters of advertisements#

Build the image using the following command

```bash
$ docker build -t simple-flask-app:latest .
```

Run the Docker container using the command shown below.

```bash
$ docker run -d -p 5000:5000 simple-flask-app
```

The application will be accessible at http:127.0.0.1:5000 or if you are using boot2docker then first find ip address using `$ boot2docker ip` and the use the ip `http://<host_ip>:5000`

#Documentation

Currently, the API supports computing the following for a given cluster
Geographic scope (Local, National, or International) -- this is computed by taking the lat lon data for every ad in the cluster, reverse geocoding them, and counting the number of unique cities, states, and countries.

Ethnicities (one / more than one) -- this is computed by taking the ethnicity field and counting.  Currently does not resolve ethnicities (spanish vs latin, etc)

Average travel speed -- for each post in the cluster which has lat lon and post time,  timestamps are sorted and the delta between each post is calculated as well as the geographic distance between the two locations.  speed = distance over time and is currently computed as mph.  If there are multiple posts with the same timestamp a second number is reported which is total distance between the concurrent posts.

mean price, price variance -- simple stats

average price quantile -- for each city in the cluster, the price quantiles are computed and the ad price’s quantile is computed.  This is averaged across all ads/cities.

number of unique cities list 
