/*
 * Copyright 2016 xxlabaza.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.xxlabaza.csv.merger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import static java.util.stream.Collectors.toList;

/**
 *
 * @author Artem Labazin (xxlabaza)
 * @since 03.02.2016
 */
public class Main {

    private final static String PROGRAM_NAME;

    private static final int LINES_PER_PASS;

    private static final String TEMPORARY_FOLDER_NAME;

    static {
        PROGRAM_NAME = "java -jar csv-merger-1.0.0.jar";
        TEMPORARY_FOLDER_NAME = "./tmp";
        LINES_PER_PASS = (int) (Runtime.getRuntime().maxMemory() / 2400);
    }

    public static void main (String[] args) throws IOException {
        System.out.format("Total memory: %dMb\n", Runtime.getRuntime().maxMemory() / 1024 / 1024);
        System.out.format("Lines per pass: %d\n", LINES_PER_PASS);

        val commandLine = parseArguments(args);

        val temporaryFolder = new File(TEMPORARY_FOLDER_NAME);
        if (!temporaryFolder.exists()) {
            temporaryFolder.mkdirs();
        }

        val inputFiles = Stream.of(commandLine.getOptionValues("input"))
                .map(Main::resolveFilePath)
                .map(File::new)
                .collect(toList());

        if (!inputFiles.stream().allMatch(File::exists)) {
            throw new IllegalArgumentException("Invalid input file");
        }

        val firstFile = new FilePartContentDescriptor(inputFiles.get(0), LINES_PER_PASS);
        val secondFile = new FilePartContentDescriptor(inputFiles.get(1), LINES_PER_PASS);

        val resultFile = Paths.get(resolveFilePath(commandLine.getOptionValue("output")));
        Files.deleteIfExists(resultFile);
        Files.createFile(resultFile);

        process(firstFile, secondFile, temporaryFolder, resultFile);
    }

    private static void process (FilePartContentDescriptor firstFile,
                                 FilePartContentDescriptor secondFile,
                                 File temporaryFolder,
                                 Path resultFile
    ) {
        firstFile.parseNextPass();
        List<String> srializedAbsoluteFilePaths = new ArrayList<>();

        do {
            secondFile.parseNextPass();
            writeIntersections(resultFile, firstFile.getValues(), secondFile.getValues());
            String serializedAbsoluteFilePath = secondFile.serializeContent(temporaryFolder);
            srializedAbsoluteFilePaths.add(serializedAbsoluteFilePath);
        } while (!secondFile.isDone());

        while (!firstFile.isDone()) {
            firstFile.parseNextPass();

            srializedAbsoluteFilePaths.stream()
                    .map(FilePartContentDescriptor::deserializeContent)
                    .forEach(deserializedMap -> {
                        writeIntersections(resultFile, firstFile.getValues(), deserializedMap);
                    });
        }
    }

    @SneakyThrows
    private static void writeIntersections (Path resultFile,
                                            Map<String, List<String>> first,
                                            Map<String, List<String>> second
    ) {
        val toWrite = first.entrySet().stream()
                .filter(entry -> {
                    return second.containsKey(entry.getKey());
                })
                .flatMap(entry -> {
                    return entry.getValue().stream()
                            .flatMap(firstValue -> {
                                return second.get(entry.getKey()).stream()
                                        .map(secondValue -> {
                                            return new StringBuilder()
                                                    .append(entry.getKey()).append(',')
                                                    .append(firstValue).append(',')
                                                    .append(secondValue)
                                                    .toString();
                                        });
                            });
                })
                .collect(toList());

        Files.write(resultFile, toWrite, StandardOpenOption.APPEND);
    }

    /*
     * Утилит-методы для парсинга входных аргументов, иными словами - ниже читать не обязательно
     */
    private static String resolveFilePath (String filePath) {
        return filePath.replaceFirst("^~", System.getProperty("user.home"));
    }

    private static CommandLine parseArguments (String[] args) {
        val options = declareOptions();
        val parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException ex) {
            System.err.format("Parsing arguments failed.\nReason: %s\n\n", ex.getMessage());
            val formatter = new HelpFormatter();
            formatter.printHelp(PROGRAM_NAME, options, true);
            System.exit(1);
        }
        return null;
    }

    private static Options declareOptions () {
        return new Options()
                .addOption(Option
                        .builder("i")
                        .longOpt("input")
                        .argName("input_files")
                        .required()
                        .desc("The two files for merge, separated by space")
                        .numberOfArgs(2)
                        .build()
                ).addOption(Option
                        .builder("o")
                        .longOpt("output")
                        .argName("result")
                        .required()
                        .desc("The output file name")
                        .hasArg()
                        .build()
                );
    }
}
