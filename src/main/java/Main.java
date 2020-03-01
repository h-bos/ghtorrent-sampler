import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.Date;

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
        String ApiUrl;
        String cloneUrl;

        int numberOfStars;

        Sample(String ApiUrl, int numberOfStars)
        {
            this.ApiUrl = ApiUrl;
            this.numberOfStars = numberOfStars;
        }
    }

    static CLIArguments CLIArguments = new CLIArguments();

    static HttpClient httpClient;
    static String authorizationHeader;

    static double totalSamplesSizeInMegaBytes = 0;
    static int totalNumberOfValidSamplesFound = 0;
    static int samplesHash = 0;

    // Stub object so we can await things in static method.
    static Object waitObject = new Object();

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

        System.out.println(
                "Sampling with arguments: \n" +
                "* Language: "                      + CLIArguments.language + "\n" +
                "* Number of samples: "             + CLIArguments.numberOfSamples + "\n" +
                "* Number of ranges: "              + CLIArguments.numberOfRanges + "\n" +
                "* Number of samples per range: "   + CLIArguments.numberOfSamplesPerRange() + "\n" +
                "* Seed: "                          + CLIArguments.seed + "\n");

        // Find properties
        Properties properties;
        try (InputStream input = ClassLoader.getSystemResourceAsStream("ghtorrent-sampler.properties"))
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
            connection = DriverManager.getConnection(
                    properties.getProperty("psql.url"),
                    properties.getProperty("psql.user"),
                    properties.getProperty("psql.password"));
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage());
            return;
        }

        // Initialize HTTP client
        httpClient = HttpClient
                .newBuilder()
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();

        authorizationHeader = properties.get("github.authorization-token").toString();

        // Sample repositories
        try
        {
            sampleRepositories(connection, CLIArguments);
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage());
            return;
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            return;
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return;
        }
    }

    static void sampleRepositories(Connection connection, CLIArguments CLIArguments) throws SQLException, IOException, InterruptedException {
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
        Random rng = new Random(CLIArguments.seed);
        // We use one RNG for each sample range because we don't want the samples subarray
        // randomly picked indices to be the same for each range.
        List<Random> rangeRNGs = new ArrayList<>();
        for (int i = 0; i < CLIArguments.numberOfRanges; i++)
        {
            rangeRNGs.add(new Random(rng.nextLong()));
        }

        List<Sample> samples = new ArrayList<>();

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
                    " (MAX: " + (CLIArguments.numberOfRanges * numberOfRepositoriesPerRange) + ")");

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

            List<Sample> validSamples = pickValidSamplesFromRange(rangeSamples);
            // If sample range was exhausted
            if (validSamples == null)
            {
                System.out.println("[ERROR] not enough valid samples where found in range. " +
                        "Please decrease the range amount or the samples amount and try again.");
                return;
            }

            samples.addAll(validSamples);

            resultSet.close();
        }
        preparedStatement.close();

        writeSamplesToFile(samples);
        calculateSamplesHash(samples);
        System.out.println("[INFO] Sampling done");
        printSamplesInfo(samples);
    }

    // Some samples will return 404 from the github API because they have been deleted. To avoid adding deleted
    // repositories to the output, we check if the repositories exist before we add them to the final list of samples.
    static List<Sample> pickValidSamplesFromRange(List<Sample> rangeSamples) throws IOException, InterruptedException {
        List<Sample> validSamples = new ArrayList<>();
        int numberOfValidSamplesFound = 0;
        int sampleIndex = 0;
        while (sampleIndex < rangeSamples.size() && numberOfValidSamplesFound < CLIArguments.numberOfSamplesPerRange())
        {
            System.out.println("[INFO] Existence checking repository: " + rangeSamples.get(sampleIndex).ApiUrl);

            HttpRequest httpRequest = HttpRequest
                    .newBuilder()
                    .uri(URI.create(rangeSamples.get(sampleIndex).ApiUrl))
                    .setHeader("Authorization", "token " + authorizationHeader)
                    .build();

            HttpResponse<String> response = httpClient
                    .send(httpRequest, HttpResponse.BodyHandlers.ofString());

            // Handle GitHub rate limiting
            long rateLimitRemaining = Long.parseLong(response.headers().firstValue("X-RateLimit-Remaining").get());
            System.out.println("[INFO] Current rate limit: " + rateLimitRemaining);
            if (rateLimitRemaining == 0)
            {
                long rateLimitReset = Long.parseLong(response.headers().firstValue("X-RateLimit-Reset").get());
                long rateLimitResetsAt = rateLimitReset * 1000;
                long deltaTime = rateLimitResetsAt - System.currentTimeMillis();
                Date timingOutUntil = Date.from(Instant.now().plusMillis(deltaTime));
                System.out.println("[INFO] GitHub rate limit reached. Timing out until " + timingOutUntil);
                synchronized (waitObject)
                {
                    waitObject.wait(deltaTime);
                }
                continue;
            }

            // If the repository doesn't exist we'll get a status code that is not 200.
            if (response.statusCode() != 200)
            {
                sampleIndex++;
                continue;
            }

            // Find clone_url value in the JSON response body
            String jsonBody = response.body();
            ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());
            JsonNode jsonRoot = jsonMapper.readTree(jsonBody);
            Iterator<Map.Entry<String, JsonNode>> nodeIterator = jsonRoot.fields();
            boolean foundCloneUrl = false;
            boolean foundRepositorySize = false;
            while (nodeIterator.hasNext() && (!foundCloneUrl || !foundRepositorySize))
            {
                Map.Entry<String, JsonNode> node = nodeIterator.next();
                if (node.getKey() == "clone_url")
                {
                    Sample validSample = rangeSamples.get(sampleIndex);
                    validSample.cloneUrl = node.getValue().asText();
                    validSamples.add(validSample);
                    foundCloneUrl = true;
                } else if (node.getKey() == "size") {
                    totalSamplesSizeInMegaBytes += node.getValue().asInt() / 1000.0;
                    foundRepositorySize = true;
                }
            }

            sampleIndex++;
            numberOfValidSamplesFound++;

            System.out.println("[INFO] Samples found: " + ++totalNumberOfValidSamplesFound + " out of " + CLIArguments.numberOfSamples);
        }

        // If the range is exhausted
        if (numberOfValidSamplesFound < CLIArguments.numberOfSamplesPerRange())
        {
            return null;
        }

        return validSamples;
    }

    static void writeSamplesToFile(List<Sample> samples)
    {
        // Write samples to file samples-{lang}.txt.
        String fileName = "samples-" + CLIArguments.language.toLowerCase() + ".txt";
        try (FileWriter writer = new FileWriter(fileName))
        {
            System.out.println("[INFO] Writing samples to file " + fileName);
            for (Sample sample : samples)
            {
                writer.write(sample.cloneUrl + "\n");
            }
        }
        catch (IOException e)
        {
            System.err.println(e.getMessage());
        }
    }

    static void calculateSamplesHash(List<Sample> samples)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (Sample sample : samples) {
            stringBuilder.append(sample);
        }
        samplesHash = stringBuilder.toString().hashCode();
    }

    static void printSamplesInfo(List<Sample> samples)
    {
        if (!samples.isEmpty())
        {
            int max = 0;
            int min = 0;
            for (Sample sample : samples)
            {
                if (sample.numberOfStars < min) min = sample.numberOfStars;
                if (sample.numberOfStars > max) max = sample.numberOfStars;
            }
            System.out.println("\nSamples info:\n" +
                    "* Max number of stars: " + max + "\n" +
                    "* Min number of stars: " + min + "\n" +
                    "* Total size of repositories if cloned: " + totalSamplesSizeInMegaBytes + " Megabytes, " +
                    totalSamplesSizeInMegaBytes / 1000 + " Gigabytes\n" +
                    "* Samples hash: " + samplesHash);
        }
    }
}
