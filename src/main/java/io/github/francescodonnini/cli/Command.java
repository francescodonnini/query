package io.github.francescodonnini.cli;

import picocli.CommandLine;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Positive;
import java.util.Optional;

public class Command implements Runnable {
    private static class Mode {
        @CommandLine.Option(names = "-D", description = "Use DataFrame API")
        boolean modeD;

        @CommandLine.Option(names = "-R", description = "Use RDD API")
        boolean modeR;
    }

    @CommandLine.Option(names = "--query", required = true, description = "Query number (1-3)")
    @Min(1)
    @Max(3)
    private int query;

    @CommandLine.Option(names = "--time", description = "Optional number of runs")
    @Positive
    private Integer time;

    @CommandLine.ArgGroup(multiplicity = "1")
    private Mode mode;

    @Override
    public void run() {
        // not used
    }

    public QueryKind getQueryKind() {
        return QueryKind.fromInt(query);
    }

    public boolean useRDD() {
        return mode.modeR;
    }

    public Optional<Integer> getTime() {
        return Optional.ofNullable(time);
    }
}
