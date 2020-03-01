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
        String apiUrl;
        String cloneUrl;

        int numberOfStars;

        Sample(String apiUrl, int numberOfStars)
        {
            this.apiUrl = apiUrl;
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
        // Simple argument parsing
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
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM project_samples WHERE language=? ORDER by nbr_of_stars");
        preparedStatement.setString(1, CLIArguments.language);
        ResultSet resultSet = preparedStatement.executeQuery();
        int numberOfRepositories = 0;
        List<Sample> repositoryPopulation = new ArrayList<>();
        while (resultSet.next())
        {
            repositoryPopulation.add(new Sample(resultSet.getString(1), resultSet.getInt(3)));
            numberOfRepositories++;
        }
        int numberOfRepositoriesPerRange = numberOfRepositories / CLIArguments.numberOfRanges;
        resultSet.close();
        preparedStatement.close();

        // Create RNGs for each repository sample range
        Random rng = new Random(CLIArguments.seed);
        List<Random> rangeRNGs = new ArrayList<>();
        for (int i = 0; i < CLIArguments.numberOfRanges; i++)
            rangeRNGs.add(new Random(rng.nextLong()));

        List<Sample> repositorySamples = new ArrayList<>();
        for (int rangeIndex = 0; rangeIndex < CLIArguments.numberOfRanges; rangeIndex++)
        {
            List<Sample> repositorySamplesFromCurrentRange = repositoryPopulation.subList(rangeIndex * numberOfRepositoriesPerRange, (rangeIndex + 1) * numberOfRepositoriesPerRange);
            // Shuffle a given range with its RNG.
            Collections.shuffle(repositorySamplesFromCurrentRange, rangeRNGs.get(rangeIndex));
            // Pick repositories that exists from range
            List<Sample> repositorySamplesFromRangeThatExist = pickExistingRepositorySamplesFromRange(repositorySamplesFromCurrentRange, CLIArguments.numberOfSamplesPerRange());
            if (repositorySamplesFromRangeThatExist == null)
            {
                System.out.println("[ERROR] not enough valid samples where found in range. " +
                        "Please decrease the range amount or the samples amount and try again.");
                return;
            }
            repositorySamples.addAll(repositorySamplesFromRangeThatExist);
        }

        calculateSamplesHash(repositorySamples);
        writeSamplesToFile(repositorySamples);
        writeMetaDataToFile(repositorySamples);
    }

    // Some samples will return 404 from the github API because they have been deleted. To avoid adding deleted
    // repositories to the output, we check if the repositories exist before we add them to the final list of samples.
    static List<Sample> pickExistingRepositorySamplesFromRange(List<Sample> rangeSamples, int numberOfSamples) throws IOException, InterruptedException {
        List<Sample> validSamples = new ArrayList<>();
        int numberOfValidSamplesFound = 0;
        int sampleIndex = 0;
        while (sampleIndex < rangeSamples.size() && numberOfValidSamplesFound < numberOfSamples)
        {
            System.out.println("[INFO] Existence checking repository: " + rangeSamples.get(sampleIndex).apiUrl);

            HttpRequest httpRequest = HttpRequest
                    .newBuilder()
                    .uri(URI.create(rangeSamples.get(sampleIndex).apiUrl))
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

    static void calculateSamplesHash(List<Sample> samples)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (Sample sample : samples) {
            stringBuilder.append(sample);
        }
        samplesHash = stringBuilder.toString().hashCode();
    }

    static void writeSamplesToFile(List<Sample> samples)
    {
        // Write samples to file samples-{lang}-{nbrOfSamples}.txt.
        String fileName = "samples-" + CLIArguments.language.toLowerCase() + "-" + CLIArguments.numberOfSamples + ".txt";
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

    static void writeMetaDataToFile(List<Sample> samples)
    {
        String meta = "";
        if (!samples.isEmpty())
        {
            int max = 0;
            int min = 0;
            for (Sample sample : samples)
            {
                if (sample.numberOfStars < min) min = sample.numberOfStars;
                if (sample.numberOfStars > max) max = sample.numberOfStars;
            }
            meta = "Meta:\n" +
                    "* Max number of stars: " + max + "\n" +
                    "* Min number of stars: " + min + "\n" +
                    "* Total size of repositories if cloned: " + totalSamplesSizeInMegaBytes + " Megabytes, " +
                    totalSamplesSizeInMegaBytes / 1000 + " Gigabytes\n" +
                    "* Samples hash: " + samplesHash;
        }
        // Write samples info to file meta-{lang}-{nbrOfSamples}.txt.
        String fileName = "meta-" + CLIArguments.language.toLowerCase() + "-" + CLIArguments.numberOfSamples + ".txt";
        try (FileWriter writer = new FileWriter(fileName))
        {
            System.out.println("[INFO] Writing samples meta to file " + fileName);
            writer.write(meta);
        }
        catch (IOException e)
        {
            System.err.println(e.getMessage());
        }

        System.out.println("\n" + meta);
    }
}
