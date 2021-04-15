# Project Option 2
CS1660 Project Option 2

Youtube Link to the demo: https://youtu.be/ySFqcCSYyWA
As of an hour before the due date the video is still "processing HD version"
If the video is too low quality to view try: 

https://pitt-my.sharepoint.com/:v:/g/personal/kek165_pitt_edu/EdAOCZD4KYxJurlebDlhTm0BLZ2ptlnTPCZqCBfmb36dFw?e=EjKUtx

Everything in the project is implemented including the extra credit with JTables and runs with any files.

# About
This is the second option course project for CS1660 Intro to Cloud Computing. This application uses Apache Hadoop and Google Cloud Platform.

The main application file is called App.java and is in src/main/java/com/mycompany/app/

The application is written in Java and as a Java Maven project. The main application is built and ran on Docker. It has a Swing GUI so to run it on a Docker image, you need to use Xming which can be done using this: https://cuneyt.aliustaoglu.biz/en/running-gui-applications-in-docker-on-windows-linux-mac-hosts/

To actually authenticate yourself for GCP, you need to download a credientials key. This can be done by following: https://cloud.google.com/docs/authentication/production#auth-cloud-explicit-java

Put the .JSON file where the .java files and Dockerfile are like how it is in the demo.

The key's file name should be replaced within the global variables as "credentialKey"

For example:
```
credentialKey = "/usr/src/myapp/key.json";
```
so replace "key.json" with your file's name

All the other variables under the "//gcp" comment should also be changed to match your projectID, bucket name, and the location of the files.
For example: replace the "gs://kek165_project" in 
```
dataLocation = "gs://kek165_project/Data-"
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

