# ghtorrent-sampler

**ghtorrent-sampler** is a tool for stratified sampling of GHTorrent repositories based on their star rating. 

## Requirements

* Java 13+
* A data dump or remote connection to a GHTorrent PSQL DB

## Get Started

**Execute the following query in the GHTorrent PSQL DB (takes a while)**

```sql
CREATE table project_samples
AS
SELECT url, language, COUNT(*) as nbr_of_stars
FROM watchers
INNER JOIN (
    SELECT id, language, url 
    FROM projects
    WHERE language='C++' OR language='Java' -- Add more languages if needed
) as projects
ON projects.id = watchers.repo_id 
GROUP BY url, language, repo_id;
```

### Configure `ghtorrent-sampler.properties`
```
psql.url=jdbc:postgresql://localhost/ghtorrent_restore
psql.user=mydbusername
psql.password=mydbpassword
github.authorization-token=mygithubtoken
```

`github.authorization-token` can be generated here: https://github.com/settings/tokens

### Build
```
mvn package
```

### Run

#### Create file with all the samples clone urls
```
java -jar ghtorrent-sampler.jar --lang C++ --nbr-of-ranges 25 --tot-nbr-of-samples 1000 --seed 1234
```

#### Clone all samples

**WARNING:** Make sure that you have enough space on your disk before you clone all repositories. 
The total size of all repositories when cloned can be found in the output.

```
# Ex. samples-java-1000.txt | xargs -n1 git clone
cat samples-{lang}-{nbrOfSamples}.txt | xargs -L1 git clone
```

## Arguments

```
--lang <String>, the language that ghtorrent-sampler filters all repositories on.
--nbr-of-ranges <int>, the number of star ranges that you want to sample from.
--tot-nbr-of-samples <int>, the total number of samples.
--seed <long>, use if you want to try to replicate the results.
```

## Replicate Run

Use the same  `--seed` argument and validate the "Samples hash" found in the output. 
The hash needs to be checked because some repositories might have been deleted from GitHub since the last run.

## Example Output

```
Sampling with arguments: 
* Language: Java
* Number of samples: 1000
* Number of ranges: 25
* Number of samples per range: 40
* Seed: 8191

{...}

Meta:
* Max number of stars: 1056
* Min number of stars: 0
* Total size of repositories if cloned: 39616.48299999995 Megabytes, 39.61648299999995 Gigabytes
* Samples hash: -1083826755
```
