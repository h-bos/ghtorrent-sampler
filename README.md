# ghtorrent-sampler

**ghtorrent-sampler** is a tool for stratified sampling of GHTorrent repositories based on their star rating. 

## Requirements

* Java 1.8+
* A data dump or remote connection to a GHTorrent PSQL DB

## Get Started

**Execute the following query in the GHTorrent PSQL DB**

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

The query is stuck? Probably not, it's just large tables and a huge join.

Why is this needed? This "staging" table improves the performance significantly.

### Configure `psql.properties`
```
url=jdbc:postgresql://localhost/ghtorrent_restore
user=myusername
password=mypassword
```

### Build
```
mvn package
```

### Run
```
java -jar ghtorrent-sampler.jar --lang C++ --nbr-of-ranges 25 --tot-nbr-of-samples 1000 --seed 1234
```

## Arguments

```
--lang <String>, the language that ghtorrent-sampler filters all repositories on.
--nbr-of-ranges <Int>, the number of star ranges that you want to sample from.
--tot-nbr-of-samples <Int>, the total number of samples.
--seed <long>, use if you want to replicate the results.
```

## Example Output

```
Running with arguments: 
* Language: C++
* Number of samples: 321
* Number of ranges: 5
* Number of samples per range: 64
* Seed: 1234

[INFO] Looking for samples in range: 0-65436 (MAX: 327180) Progress: 0%
[INFO] Looking for samples in range: 65436-130872 (MAX: 327180) Progress: 25%
[INFO] Looking for samples in range: 130872-196308 (MAX: 327180) Progress: 50%
[INFO] Looking for samples in range: 196308-261744 (MAX: 327180) Progress: 75%
[INFO] Looking for samples in range: 261744-327180 (MAX: 327180) Progress: 100%
[INFO] Writing samples to file samples.txt.

Sample statistics, Max number of stars: 935 Min number of stars: 0
```
