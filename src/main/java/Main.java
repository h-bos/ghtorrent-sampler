import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
            return cliArguments.numberOfSamples / cliArguments.numberOfRanges;
        }
    }

    static class RepositorySample
    {
        String apiUrl;
        String cloneUrl;

        int numberOfStars;

        RepositorySample(String apiUrl, int numberOfStars)
        {
            this.apiUrl = apiUrl;
            this.numberOfStars = numberOfStars;
        }
    }

    static CLIArguments cliArguments = new CLIArguments();

    static HttpClient httpClient;
    static String authorizationHeader;

    static double totalSamplesSizeInKiloBytes = 0;
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
                    cliArguments.language = args[i+1];
                    break;
                case "--nbr-of-ranges":
                    cliArguments.numberOfRanges = Integer.parseInt(args[i+1]);
                    break;
                case "--tot-nbr-of-samples":
                    cliArguments.numberOfSamples = Integer.parseInt(args[i+1]);
                    break;
                case "--seed":
                    cliArguments.seed = Long.parseLong(args[i+1]);
                    break;
                default:
                    System.err.println("Invalid argument " + args[i]);
                    return;
            }
        }

        System.out.println(
                "Sampling with arguments: \n" +
                "* Language: "                      + cliArguments.language + "\n" +
                "* Number of samples: "             + cliArguments.numberOfSamples + "\n" +
                "* Number of ranges: "              + cliArguments.numberOfRanges + "\n" +
                "* Number of samples per range: "   + cliArguments.numberOfSamplesPerRange() + "\n" +
                "* Seed: "                          + cliArguments.seed + "\n");

        // Find properties
        Properties properties;
        try (InputStream input = ClassLoader.getSystemResourceAsStream("ghtorrent-sampler.properties"))
        {
            properties = new Properties();
            properties.load(input);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }

        // Connect to DB
        try (Connection connection = DriverManager.getConnection
                (
                    properties.getProperty("psql.url"),
                    properties.getProperty("psql.user"),
                    properties.getProperty("psql.password")
                )
            )
        {
            // Initialize HTTP client
            httpClient = HttpClient
                    .newBuilder()
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .build();

            authorizationHeader = properties.get("github.authorization-token").toString();

            sampleRepositories(connection, cliArguments);
        }
        catch (SQLException | InterruptedException | IOException e)
        {
            e.printStackTrace();
            return;
        }
    }

    static void sampleRepositories(Connection connection, CLIArguments cliArguments) throws SQLException, IOException, InterruptedException {
        // Find repository population
        List<RepositorySample> repositoryPopulation = new ArrayList<>();
        int numberOfRepositoriesPerRange = 0;
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM project_samples WHERE language=? ORDER by nbr_of_stars"))
        {
            preparedStatement.setString(1, cliArguments.language);
            try (ResultSet resultSet = preparedStatement.executeQuery())
            {
                int numberOfRepositories = 0;
                while (resultSet.next())
                {
                    repositoryPopulation.add(new RepositorySample(resultSet.getString(1), resultSet.getInt(3)));
                    numberOfRepositories++;
                }
                numberOfRepositoriesPerRange = numberOfRepositories / cliArguments.numberOfRanges;
            }
        }

        // Create RNGs for each repository sample range
        Random rng = new Random(cliArguments.seed);
        List<Random> rangeRNGs = new ArrayList<>();
        for (int i = 0; i < cliArguments.numberOfRanges; i++)
            rangeRNGs.add(new Random(rng.nextLong()));

        List<RepositorySample> repositorySamples = new ArrayList<>();

        // For each repository range, pick {--tot-nbr-of-samples}/{--nbr-of-ranges} random repositories that exist according to the GitHub API.
        for (int rangeIndex = 0; rangeIndex < cliArguments.numberOfRanges; rangeIndex++)
        {
            // Create list of repositories from the current range
            List<RepositorySample> repositorySamplesFromCurrentRange = repositoryPopulation.subList(rangeIndex * numberOfRepositoriesPerRange, (rangeIndex + 1) * numberOfRepositoriesPerRange);
            // Shuffle a given range with its RNG
            Collections.shuffle(repositorySamplesFromCurrentRange, rangeRNGs.get(rangeIndex));
            // Pick repositories that exists from range
            List<RepositorySample> repositorySamplesFromRangeThatExist = pickExistingRepositorySamplesFromRange(repositorySamplesFromCurrentRange, cliArguments.numberOfSamplesPerRange());
            // If not enough existing repositories could be found in range
            if (repositorySamplesFromRangeThatExist == null)
            {
                System.out.println("[ERROR] not enough valid samples where found in range. " +
                        "Please decrease the range amount or the samples amount and try again.");
                return;
            }
            repositorySamples.addAll(repositorySamplesFromRangeThatExist);
        }

        calculateSamplesHash(repositorySamples);
        writeRepositorySamplesCloneUrlsToFile(repositorySamples);
        writeRepositorySamplesMetaDataToFile(repositorySamples);
    }

    // Some samples will return 404 from the github API because they have been deleted. To avoid adding deleted
    // repositories to the output, we check if the repositories exist before we add them to the final list of samples.
    static List<RepositorySample> pickExistingRepositorySamplesFromRange(List<RepositorySample> rangeRepositorySamples, int numberOfRepositories) throws IOException, InterruptedException {
        List<RepositorySample> foundExistingRepositorySamples = new ArrayList<>();
        int numberOfExistingRepositoriesFound = 0;
        int repositoryIndex = 0;
        while (repositoryIndex < rangeRepositorySamples.size() && numberOfExistingRepositoriesFound <  numberOfRepositories)
        {

            HttpRequest httpRequest = HttpRequest
                    .newBuilder()
                    .uri(URI.create(rangeRepositorySamples.get(repositoryIndex).apiUrl))
                    .setHeader("Authorization", "token " + authorizationHeader)
                    .build();

            HttpResponse<String> response = httpClient
                    .send(httpRequest, HttpResponse.BodyHandlers.ofString());

            // Handle GitHub rate limiting
            long rateLimitRemaining = Long.parseLong(response.headers().firstValue("X-RateLimit-Remaining").get());
            System.out.println("[INFO] Current GitHub rate limit: " + rateLimitRemaining);
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
                System.out.println("[INFO] Repository missing " + rangeRepositorySamples.get(repositoryIndex).apiUrl);
                repositoryIndex++;
                continue;
            }

            System.out.println("[INFO] Repository found " + rangeRepositorySamples.get(repositoryIndex).apiUrl);

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
                if (node.getKey().equals("clone_url"))
                {
                    RepositorySample validRepositorySample = rangeRepositorySamples.get(repositoryIndex);
                    validRepositorySample.cloneUrl = node.getValue().asText();
                    foundExistingRepositorySamples.add(validRepositorySample);
                    foundCloneUrl = true;
                } else if (node.getKey().equals("size")) {
                    totalSamplesSizeInKiloBytes += node.getValue().asInt();
                    foundRepositorySize = true;
                }
            }

            repositoryIndex++;
            numberOfExistingRepositoriesFound++;

            System.out.println("[INFO] Samples found: " + ++totalNumberOfValidSamplesFound + " out of " + cliArguments.numberOfSamples);
        }

        // If the range is exhausted
        if (numberOfExistingRepositoriesFound < cliArguments.numberOfSamplesPerRange())
        {
            return null;
        }

        return foundExistingRepositorySamples;
    }

    static void calculateSamplesHash(List<RepositorySample> repositorySamples)
    {
        StringBuilder stringBuilder = new StringBuilder();
        for (RepositorySample repositorySample : repositorySamples) {
            stringBuilder.append(repositorySample);
        }
        samplesHash = stringBuilder.toString().hashCode();
    }

    static void writeRepositorySamplesCloneUrlsToFile(List<RepositorySample> repositorySamples)
    {
        // Write samples to file samples-{lang}-{nbrOfSamples}.txt.
        String fileName = "samples-" + cliArguments.language.toLowerCase() + "-" + cliArguments.numberOfSamples + ".txt";
        try (FileWriter writer = new FileWriter(fileName))
        {
            System.out.println("[INFO] Writing samples to file " + fileName);
            for (RepositorySample repositorySample : repositorySamples)
            {
                String[] cloneUrlParts = repositorySample.cloneUrl.split("/");
                // uniqueRepositoryName='{username}-{repositoryName}'
                String uniqueRepositoryName = cloneUrlParts [3] + "-" + cloneUrlParts[4].split("\\.")[0];
                writer.write(repositorySample.cloneUrl + " " + uniqueRepositoryName + "\n");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    static void writeRepositorySamplesMetaDataToFile(List<RepositorySample> repositorySamples)
    {
        String meta = "";
        if (!repositorySamples.isEmpty())
        {
            int max = 0;
            int min = 0;
            for (RepositorySample repositorySample : repositorySamples)
            {
                if (repositorySample.numberOfStars < min) min = repositorySample.numberOfStars;
                if (repositorySample.numberOfStars > max) max = repositorySample.numberOfStars;
            }
            meta = "Meta:\n" +
                    "* Max number of stars: " + max + "\n" +
                    "* Min number of stars: " + min + "\n" +
                    "* Total size of repositories if cloned: " + totalSamplesSizeInKiloBytes / 1000 + " Megabytes, " +
                    totalSamplesSizeInKiloBytes / 1000000 + " Gigabytes\n" +
                    "* Samples hash: " + samplesHash;
        }
        // Write samples info to file meta-{lang}-{nbrOfSamples}.txt.
        String fileName = "meta-" + cliArguments.language.toLowerCase() + "-" + cliArguments.numberOfSamples + ".txt";
        try (FileWriter writer = new FileWriter(fileName))
        {
            System.out.println("[INFO] Writing samples meta to file " + fileName);
            writer.write(meta);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return;
        }

        System.out.println("\n" + meta);
    }
}
