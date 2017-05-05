package com.hazelcast.satimulus.filter;

import com.hazelcast.jet.AbstractProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class FileP extends AbstractProcessor{
    private String fileName;

    public FileP(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public boolean complete() {
        return tryEmit(licensesFromFile(fileName).collect(toSet()));
    }

    private static Stream<String> licensesFromFile(String name) {
        try {
            return Files.lines(Paths.get(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
