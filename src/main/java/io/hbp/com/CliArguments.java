package io.hbp.com;

public class CliArguments
{
    public String language;
    public int numberOfSamples;
    public int numberOfRanges;
    public long seed;

    public int numberOfSamplesPerRange()
    {
        return numberOfSamples / numberOfRanges;
    }
}
