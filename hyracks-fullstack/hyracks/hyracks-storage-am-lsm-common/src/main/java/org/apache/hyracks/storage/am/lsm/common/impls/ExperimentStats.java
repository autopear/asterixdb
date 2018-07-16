package org.apache.hyracks.storage.am.lsm.common.impls;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by mohiuddin on 7/9/18.
 */
public class ExperimentStats {

    public static int LOG_POINT = 10;
    // "/home/mohiuddin/asterix-hyracks/Results/stats_";
    public String LOG_NAME = "/home/mohiuddin/asterix-hyracks/Results/stats_";
    public String MERGELATENCY_LOG = "/home/mohiuddin/asterix-hyracks/stats_latency_";
    long numberofMerges;
    long numberofFlushes;
    double flushSizeInBytes;
    double mergeSizeInBytes;
    double sumOfMergeLatencyInSeconds;
    ArrayList<Double> mergeLatencyInSeconds;
    //ArrayList<Double> writeAmplificationSnapshot;

    public ExperimentStats(String variantName) {
        this.mergeLatencyInSeconds = new ArrayList<>();
        //writeAmplificationSnapshot = new ArrayList<>();
        numberofMerges = 0;
        numberofFlushes = 0;
        flushSizeInBytes = 0;
        mergeSizeInBytes = 0;
        sumOfMergeLatencyInSeconds = 0;
        LOG_NAME+=variantName;
        MERGELATENCY_LOG+=variantName;
        WriteInFile("No Of Flush, No Of Merge, Write Amplification, Avg Merge Latency, Total Merge Time", LOG_NAME);
    }

    public void UpdateFlushStats(long size)
    {
        if(numberofFlushes%LOG_POINT == 0 && numberofFlushes!=0)
            TakeSnapshot();
        numberofFlushes++;
        flushSizeInBytes+=size;

    }

    private void TakeSnapshot() {
        String stats = numberofFlushes + "," + numberofMerges + "," + (mergeSizeInBytes)/flushSizeInBytes + "," + (numberofMerges>0? sumOfMergeLatencyInSeconds/numberofMerges : 0) + "," + sumOfMergeLatencyInSeconds;
        WriteInFile(stats, LOG_NAME);
        for(Double d: mergeLatencyInSeconds)
        {
            WriteInFile(""+d, MERGELATENCY_LOG);
        }
        mergeLatencyInSeconds.clear();
    }

    public void UpdateMergeStats(long size, double timeInSeconds)
    {
        numberofMerges++;
        mergeSizeInBytes+=size;
        mergeLatencyInSeconds.add(timeInSeconds);
        sumOfMergeLatencyInSeconds+=timeInSeconds;
    }

    void WriteInFile(String stat, String filename)
    {
        try {
            // Assume default encoding.
            File file = new File(filename);
            FileWriter fileWriter;
            if(file.exists()) {

                fileWriter = new FileWriter(file, true);
            }
            else
            {
                file.createNewFile();
                fileWriter = new FileWriter(file, false);
            }
            BufferedWriter bufferedWriter =
                    new BufferedWriter(fileWriter);


            bufferedWriter.write(stat);
            bufferedWriter.newLine();


            // Always close files.
            bufferedWriter.close();
        }
        catch(IOException ex) {
            System.out.println(
                    "Error writing to file '"
                            + filename + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
    }

}
