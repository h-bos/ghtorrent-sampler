package io.hbp.com;

public class RepositorySample
{
    public String apiUrl;
    public String cloneUrl;

    public int numberOfStars;

    public RepositorySample(String apiUrl, int numberOfStars)
    {
        this.apiUrl = apiUrl;
        this.numberOfStars = numberOfStars;
    }

    @Override
    public String toString()
    {
        return apiUrl + " number of stars: " + numberOfStars;
    }
}
