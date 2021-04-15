# Project Option 2
CS1660 Project Option 2

Youtube Link to the demo: https://youtu.be/ySFqcCSYyWA

Everything in the project is implemented including the extra credit with JTables and runs with any files.

# About
This is the second option course project for CS1660 Intro to Cloud Computing. This application uses Apache Hadoop and Google Cloud Platform.

The application is written in Java and as a Java Maven project. The main application is built and ran on Docker. It has a Swing GUI so to run it on a Docker image, you need to use Xming which can be done using this: https://cuneyt.aliustaoglu.biz/en/running-gui-applications-in-docker-on-windows-linux-mac-hosts/

To actually authenticate yourself for GCP, you need to download a credientials key. This can be done by following: https://cloud.google.com/docs/authentication/production#auth-cloud-explicit-java

The key's file location should be replaced within the global variables as "credentialKey"

For example:
```
credentialKey = "/usr/src/myapp/key.json";
```
Where key.json is the downloaded key

All the other variables under the "//gcp" comment should also be changed to match your projectID, bucket name, and the location of the files.
For example: replace the "gs://kek165_project" in 
```
[dataLocation = "gs://kek165_project/Data-"]
```

with your bucket url instead

In the demo video I had it so that you had to change the cluster name in the method. After recording, I changed it so that it is a global variable and only needs to be changed there.

The InvertIndex.java and TopN.java must also be uploaded to your GCP bucket as JAR files.

The Dockerfile included is used to run the Docker commands:

To build:
```
docker build -t project .
```

To run:
```
docker run --rm -it -e DISPLAY=YOUR_IP:0.0 project
```

