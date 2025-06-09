package io.github.francescodonnini.cli;

import picocli.CommandLine;

import javax.validation.constraints.Positive;
import java.util.Optional;

public class Command implements Runnable {
    @CommandLine.Option(names = "--query", required = true, description = "Query id (1D, 1R, 1S, 2D, 2R, 2S)")
    private String query;

    @CommandLine.Option(names = "--time", description = "Optional number of runs")
    @Positive
    private Integer time;

    @CommandLine.Option(names = "--appName", description = "Name of the Spark Application")
    private String appName;

    @Override
    public void run() {
        // unused
    }

    public QueryKind getQueryKind() {
        return QueryKind.fromString(query);
    }

    public Optional<Integer> getTime() {
        return Optional.ofNullable(time);
    }

    public Optional<String> getAppName() {
        return Optional.ofNullable(appName);
    }
}
