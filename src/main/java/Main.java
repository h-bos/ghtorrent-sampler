import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class Main
{
    static class CLIArguments
    {
        String language;
        int numberOfSamples;
        int numberOfRanges;
        long seed;

        static int numberOfSamplesPerRange()
        {
            return CLIArguments.numberOfSamples / CLIArguments.numberOfRanges;
        }
    }

    static class Sample
    {
        String url;
        int numberOfStars;

        Sample(String url, int numberOfStars)
        {
            this.url = url;
            this.numberOfStars = numberOfStars;
        }
    }

    static CLIArguments CLIArguments = new CLIArguments();

    public static void main(String[] args)
    {
        // Simple argument parsing (I know that third party tools exist for argument parsing but I don't want to use them.)
        for (int i = 0; i < args.length; i += 2)
        {
            switch (args[i])
            {
                case "--lang":
                    CLIArguments.language = args[i+1];
                    break;
                case "--nbr-of-ranges":
                    CLIArguments.numberOfRanges = Integer.parseInt(args[i+1]);
                    break;
                case "--tot-nbr-of-samples":
                    CLIArguments.numberOfSamples = Integer.parseInt(args[i+1]);
                    break;
                case "--seed":
                    CLIArguments.seed = Long.parseLong(args[i+1]);
                    break;
                default:
                    System.err.println("Invalid argument " + args[i]);
                    return;
            }
        }

        System.out.println("Sampling with arguments: ");
        System.out.println("* Language: " + CLIArguments.language);
        System.out.println("* Number of samples: " + CLIArguments.numberOfSamples);
        System.out.println("* Number of ranges: " + CLIArguments.numberOfRanges);
        System.out.println("* Number of samples per range: " + CLIArguments.numberOfSamplesPerRange());
        System.out.println("* Seed: " + CLIArguments.seed + "\n");

        // Find properties
        Properties properties;
        try (InputStream input = ClassLoader.getSystemResourceAsStream("psql.properties"))
        {
            properties = new Properties();
            properties.load(input);
        }
        catch (FileNotFoundException e)
        {
            System.err.println(e.getMessage());
            return;
        }
        catch (IOException e)
        {
            System.err.println(e.getMessage());
            return;
        }

        // Connect to DB
        Connection connection;
        try
        {
             connection = DriverManager.getConnection(properties.getProperty("url"), properties);
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage());
            return;
        }

        // Sample repositories
        try
        {
            sampleRepositories(connection, CLIArguments);
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage());
        }
    }

    static void sampleRepositories(Connection connection, CLIArguments CLIArguments) throws SQLException
    {
        // Find the number of repositories
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM project_samples WHERE language=?");
        preparedStatement.setString(1, CLIArguments.language);
        ResultSet resultSet = preparedStatement.executeQuery();
        int numberOfRepositories = 0;
        while (resultSet.next())
        {
            numberOfRepositories = Integer.parseInt(resultSet.getString(1));
        }
        resultSet.close();
        preparedStatement.close();
        int numberOfRepositoriesPerRange = numberOfRepositories / CLIArguments.numberOfRanges;


        // Create RNGs
        List<Sample> samples = new ArrayList<>();
        Random rng = new Random(CLIArguments.seed);
        // We use one RNG for each sample range because we don't want the samples subarray
        // randomly picked indices to be the same for each range.
        List<Random> rangeRNGs = new ArrayList<>();
        for (int i = 0; i < CLIArguments.numberOfRanges; i++)
        {
            rangeRNGs.add(new Random(rng.nextLong()));
        }

        // This query selects samples within a range [OFFSET, OFFSET + LIMIT] for a specified language.
        preparedStatement = connection.prepareStatement(
                "SELECT * FROM project_samples WHERE language=? ORDER BY nbr_of_stars DESC LIMIT ? OFFSET ?;");

        // For each range, pick random samples from each query and put into the list of all samples.
        for (int i = 0; i < CLIArguments.numberOfRanges; i++)
        {
            System.out.println("[INFO] Looking for samples in range: "
                    // Ex output: 149089-298178
                    + numberOfRepositoriesPerRange * i + "-" + numberOfRepositoriesPerRange * (i + 1) +
                    // Ex output: (MAX: 745445)
                    " (MAX: " + (CLIArguments.numberOfRanges * numberOfRepositoriesPerRange) + ")"
                    // Ex output: Progress: 25%
                    + " Progress: " + (int)((double)i / (CLIArguments.numberOfRanges - 1) * 100) + "%");

            // Set statement parameters (SQL injection safe just in case someone decides to use this program on a non-local server).
            preparedStatement.setString(1, CLIArguments.language);
            preparedStatement.setInt(2, numberOfRepositories);
            preparedStatement.setInt(3, numberOfRepositoriesPerRange * i);
            resultSet = preparedStatement.executeQuery();

            // Shuffle range and pick samples.
            List<Sample> rangeSamples = new ArrayList<>();
            while(resultSet.next())
            {
                rangeSamples.add(new Sample(resultSet.getString(1), Integer.parseInt(resultSet.getString(3))));
            }

            // Pick random range samples and put into the samples list.
            Collections.shuffle(rangeSamples, rangeRNGs.get(i));
            samples.addAll(rangeSamples.subList(0, CLIArguments.numberOfSamplesPerRange()));
            resultSet.close();
        }
        preparedStatement.close();

        writeSamplesToFile(samples);
    }

    static void writeSamplesToFile(List<Sample> samples)
    {
        // Write samples to file samples.txt.
        try (FileWriter writer = new FileWriter("samples.txt"))
        {
            System.out.println("[INFO] Writing samples to file samples.txt.");
            for (Sample sample : samples)
            {
                writer.write(sample.url + "\n");
            }
        }
        catch (IOException e)
        {
            System.err.println(e.getMessage());
        }

        // Print sample statistics
        if (!samples.isEmpty())
        {
            int max = 0;
            int min = 0;
            for (Sample sample : samples)
            {
                if (sample.numberOfStars < min) min = sample.numberOfStars;
                if (sample.numberOfStars > max) max = sample.numberOfStars;
            }
            System.out.println("\nSample statistics:");
            System.out.println("* Max number of stars: " + max);
            System.out.println("* Min number of stars: " + min);
        }
    }
}
