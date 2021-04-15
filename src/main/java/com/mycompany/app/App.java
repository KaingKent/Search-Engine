package com.mycompany.app;

import java.awt.*;
import java.awt.event.*;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import javax.swing.*;
import javax.swing.table.JTableHeader;

import java.io.*;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobReference;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


//docker build -t project .
//docker run --rm -it -e DISPLAY=192.168.56.1:0.0 project

//https://stackoverflow.com/questions/1089285/maven-run-project
//https://stackoverflow.com/questions/27767264/how-to-dockerize-maven-project-and-how-many-ways-to-accomplish-it
public class App{
    private static File[] files;
    private static JLabel title, label2 = new JLabel();;
    private static Storage storage;
    private static GoogleCredentials credentials;

    //gcp
    private static final String projectID = "crucial-engine-305200";
    private static final String bucketName = "kek165_project";
    private static final String credentialKey = "/usr/src/myapp/key.json";
    private static final String invertIndexJarLocation = "gs://kek165_project/InvertIndex.jar";
    private static final String dataLocation = "gs://kek165_project/Data-";
    private static final String outputLocation = "gs://kek165_project/output-";
    private static final String TopNLocation = "gs://kek165_project/TopN.jar";
    private static final String TopNData = "gs://kek165_project/output-";
    private static final String TopNOutput = "gs://kek165_project/topN-output-";
    private static final String clusterName = "cluster-765b";

    //jobs
    private static String jobId;
    private static String jobId2;
    private static Job job;
    private static Dataproc dataproc;

    //gui
    private static JFrame frame = new JFrame("Project Option 2");
    private static JPanel panel = new JPanel();
    private static JButton button = new JButton("Choose Files");
    private static JButton button2 = new JButton("Load Engine");
    private static JButton button3 = new JButton("Search for Term");
    private static JButton button4 = new JButton("Top-N");
    private static JButton button5 = new JButton("Go back");
    private static JTextField field = new JTextField();
    private static JTextField search = new JTextField();
    private static JTable table;
    private static JScrollPane sp;

    //results
    private static String topNumber;
    private static String[][] dataArr;
    private static String[][] searchDataArr;


    public static void main(String[] args) throws IOException{
        
        //open file explorer
        button.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                System.out.println("getting files");
                openFileExplorer();
            }
        });

        //button to invert indices
        button2 = new JButton("Load Engine");
        button2.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e){
                System.out.println("uploading files");
                try{
                    jobId = UUID.randomUUID().toString();
                    authExplicit(credentialKey);
                    uploadFiles();
                    System.out.println("uploaded");
                    title.setText("Running job");
                    createJob();
                    waitForJob("invertIndex-", "Engine Loaded", jobId);
                    panel.remove(button);

                    panel.remove(button2);
                    label2.setText("");

                    button3.setBounds(315, 350, 150, 40);
                    button4.setBounds(290,450,200,40);

                    panel.add(button3);
                    panel.add(button4);

                    SwingUtilities.updateComponentTreeUI(frame);
                }catch(Exception error){
                    System.out.println(error);
                }
            }
        });

        //button to search for a term
        button3.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                button3.setVisible(false);
                button4.setVisible(false);
                panel.add(search);
                search.setVisible(true);
                title.setText("Search for a term");
                search.setBounds(315, 350, 150, 40);
            }
        });

        //textfield for searching for a term
        search.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                String searchTerm = search.getText().toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
                System.out.println("Searching for Term");

                searchData(searchTerm);

                search.setVisible(false);
                panel.setVisible(false);

                String[] colNames = {"Term", "Document", "Frequency"};
                JTable searching = new JTable(searchDataArr, colNames);
                searching.setBounds(200, 360, 350, 300);

                sp = new JScrollPane(searching);
                sp.setSize(new Dimension(500, 500));
                button5.setBounds(290,700,200,40);
                frame.add(button5);
                button5.setVisible(true);
                frame.add(sp);
            }
        });

        //button to find top-n
        button4.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                title.setText("Top-N");
                button3.setVisible(false);
                button4.setVisible(false);
                panel.add(field);
                field.setVisible(true);
                field.setBounds(315, 350, 150, 40);
            }
        });

        //textfield to get the n in top-n
        field.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                jobId2 = UUID.randomUUID().toString();
                topNumber = field.getText();

                System.out.println("running topN");
                createTopNJob();
                waitForJob("topN-", "Top-N Loaded", jobId2);
                storeData();

                field.setVisible(false);
                panel.setVisible(false);

                String[] colNames = {"Frequency", "Term"};

                JTable topN = new JTable(dataArr, colNames);
                topN.setBounds(300, 360, 200, 300);

                sp = new JScrollPane(topN);
                sp.setPreferredSize(new Dimension(500,500));
                button5.setBounds(290,700,200,40);
                frame.add(button5);
                button5.setVisible(true);
                frame.add(sp);
            }
        });

        //the go back button
        button5.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e){
                title.setText("Pick an option");
                panel.setVisible(true);
                button5.setVisible(false);
                frame.remove(sp);
                button3.setVisible(true);
                button4.setVisible(true);
            }
        });

        button.setBounds(315, 350, 150, 40);
        button2.setBounds(290,450,200,40);

        JLabel label = new JLabel("Kent's Search Engine");
        label.setBounds(10, 10, 200, 20);
        title = new JLabel("Load My Engine");
        title.setBounds(305,200,250,70);
        title.setFont(new Font("verdana", Font.BOLD, 20 ));
        label2.setFont(new Font("verdana", Font.PLAIN, 10 ));
        label2.setBounds(305,245,250,70);
        label2.setText("<html>");

        panel.setLayout(null);
        panel.add(label);
        panel.add(title);
        panel.add(label2);
        panel.add(button);
        panel.add(button2);
        
        frame.add(panel, BorderLayout.CENTER);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
        frame.setSize(800,800);
    }

    //google authentication
    private static void authExplicit(String jsonPath) throws IOException {//https://cloud.google.com/docs/authentication/production#auth-cloud-explicit-java
        credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
              .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
      }
    
    //open the file explorer and grab the files
    private static void openFileExplorer(){
        JFileChooser chooser = new JFileChooser();
        chooser.setMultiSelectionEnabled(true);

        int i = chooser.showSaveDialog(null);

        if(i == chooser.APPROVE_OPTION){
            files = chooser.getSelectedFiles();//stores the files

            for(int j = 0; j < files.length; j++){//prints the files to the console and gui
                System.out.println(files[j].getAbsolutePath());
                label2.setText(label2.getText() + "<br>" + files[j].getName());

            }
            label2.setText(label2.getText() + "</html>");
        }else{
            System.out.println("No files");
        }
    }

    //uploads the files to the bucket
    private static void uploadFiles() throws IOException{//https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-java
        for(int i = 0; i < files.length; i++){
            BlobId blobId = BlobId.of(bucketName, "Data-"+ jobId +"/" + files[i].getName());
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            storage.create(blobInfo, Files.readAllBytes(Paths.get(files[i].getAbsolutePath())));
            System.out.println("File " + files[i].getAbsolutePath() + " uploaded to bucket " + bucketName + " as " + files[i].getName());
        }
    }

    //https://developers.google.com/resources/api-libraries/documentation/dataproc/v1/java/latest/com/google/api/services/dataproc/model/HadoopJob.html
    //https://developers.google.com/resources/api-libraries/documentation/dataproc/v1/java/latest/com/google/api/services/dataproc/model/Job.html
    //https://cloud.google.com/dataproc/docs/samples/dataproc-submit-hadoop-fs-job
    //creates a job on the cluster for inverting the indices
    private static void createJob() {
        job = null;
        try {
            dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpCredentialsAdapter(credentials)).setApplicationName("My First Project").build();//gets the project name

            JobPlacement placement = new JobPlacement().setClusterName(clusterName);//finds the specified cluster
            HadoopJob hadoopJob = new HadoopJob().setMainClass("InvertIndex").setJarFileUris(ImmutableList.of(invertIndexJarLocation))
                                    .setArgs(ImmutableList.of(dataLocation + jobId, outputLocation + jobId)); //instantiates the hadoop job

            job = dataproc.projects().regions().jobs().submit(projectID, "us-central1",
                    new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId("invertIndex-" + jobId))
                    .setPlacement(placement).setHadoopJob(hadoopJob))).execute(); //creates the job on the specified cluster
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    //same as create job but for top-n
    private static void createTopNJob(){
        job = null;
        try {
            dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpCredentialsAdapter(credentials)).setApplicationName("My First Project").build();

            JobPlacement placement = new JobPlacement().setClusterName(clusterName);
            HadoopJob hadoopJob = new HadoopJob().setMainClass("TopN").setJarFileUris(ImmutableList.of(TopNLocation))
                                    .setArgs(ImmutableList.of(TopNData + jobId + "/part-r-00000", TopNOutput + jobId2, topNumber));

            job = dataproc.projects().regions().jobs().submit(projectID, "us-central1",
                    new SubmitJobRequest().setJob(new Job().setReference(new JobReference().setJobId("topN-" + jobId2))
                    .setPlacement(placement).setHadoopJob(hadoopJob))).execute();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    //waits for the job to finish
    private static void waitForJob(String name, String newText, String id){
        while(true){
            try{
                JobStatus status = dataproc.projects().regions().jobs().get(projectID, "us-central1", name + id).execute().getStatus(); //gets the status of the job

                if(status.getState().compareTo("DONE") == 0){//checks if its done
                    title.setText(newText);
                    break;
                }
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }

    //grabs the data for top n
    private static void storeData(){
        try{
            Blob blob = storage.get(bucketName, "topN-output-" + jobId2 + "/part-r-00000");
            String fileContent = new String(blob.getContent());
            convertData(fileContent);
        }catch (Exception e){
            System.out.println(e);
        }

    }

    //converts the data for top-n into a 2-d array
    private static void convertData(String data){
        String[] tokens = data.split("\n");
        dataArr = new String[tokens.length][2];

        for(int i = 0; i < tokens.length; i++){
            String[] t = tokens[i].split("\t");
            dataArr[i] = t;
        }
    }

    //grabs the data for searching
    private static void searchData(String key){
        try{
            Blob blob = storage.get(bucketName, "output-" + jobId + "/part-r-00000");
            String fileContent = new String(blob.getContent());
            search(fileContent, key);
        }catch (Exception e){
            System.out.println(e);
        }
    }

    //searches for the word
    private static void search(String data, String key){
        HashMap<String, ArrayList<String>> wordToDoc = new HashMap<>();

        String[] tokens = data.split("\n");//splits the data into each line
        for(int i = 0; i < tokens.length; i++){
            String[] tokens2 = tokens[i].split("\t");//splits the line into words

            if(wordToDoc.containsKey(tokens2[0])){//if the word is already there then add the file name + count
                wordToDoc.get(tokens2[0]).add(tokens2[1] + ":" + tokens2[2]);       
            }else{//if its a new word make a new arraylist and add the word and file name + count
                ArrayList<String> arr = new ArrayList<>();
                arr.add(tokens2[1] + ":" + tokens2[2]);
                wordToDoc.put(tokens2[0], arr);
            }
        }

        //populates the searchDataArr with the occurences of the key
        if(wordToDoc.containsKey(key)){
            int k = 0;
            searchDataArr = new String[wordToDoc.get(key).size()][3]; 
            for(String s : wordToDoc.get(key)){
                String[] t = s.split(":");
                String[] temp = new String[3];
                temp[0] = key;
                temp[1] = t[0];
                temp[2] = t[1];
                searchDataArr[k] = temp;
                k++;
            }
        }
    }
}
