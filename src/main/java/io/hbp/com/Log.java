package io.hbp.com;

import java.io.FileWriter;
import java.io.IOException;

public class Log
{
    static StringBuilder stringBuilder = new StringBuilder();

    static void info(String info)
    {
        String message = "[INFO] " + info;
        System.out.println(message);
        stringBuilder.append(message);
        stringBuilder.append(System.getProperty("line.separator"));
    }

    static void error(String error)
    {
        String message = "[ERROR] " + error;
        System.out.println(message);
        stringBuilder.append(message);
        stringBuilder.append(System.getProperty("line.separator"));
    }

    static void writeToFile(String fileName)
    {
        try (FileWriter writer = new FileWriter(fileName))
        {
            Log.info("Writing log to file " + fileName);
            writer.write(stringBuilder.toString());
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }
    }
}
