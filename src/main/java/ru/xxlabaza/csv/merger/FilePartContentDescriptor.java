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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;

/**
 *
 * @author Artem Labazin (a.labazin)
 * @since 03.02.2016
 */
@Data
class FilePartContentDescriptor {

    @SneakyThrows
    static Map<String, List<String>> deserializeContent (String absoluteFilePath) {
        try (val fileInputStream = new FileInputStream(absoluteFilePath);
             val objectInputStream = new ObjectInputStream(fileInputStream)) {

            return (HashMap<String, List<String>>) objectInputStream.readObject();
        }
    }

    private final File file;

    private final Map<String, List<String>> values;

    private final int readLinesPerPass;

    private long filePointer;

    private boolean isDone;

    FilePartContentDescriptor (File file, int readLinesPerPass) {
        this.file = file;
        this.readLinesPerPass = readLinesPerPass;
        values = new HashMap<>(readLinesPerPass);
    }

    @SneakyThrows
    void parseNextPass () {
        if (isDone) {
            return;
        }

        values.clear();
        try (val randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(filePointer);

            for (int count = 0; count < readLinesPerPass; count++) {
                val line = randomAccessFile.readLine();
                if (line == null) {
                    isDone = true;
                    break;
                }

                val tokens = line.split(",");
                if (values.containsKey(tokens[0])) {
                    values.get(tokens[0]).add(tokens[1]);
                } else {
                    List<String> list = new ArrayList<>(1);
                    list.add(tokens[1]);
                    values.put(tokens[0], list);
                }
            }

            filePointer = randomAccessFile.getFilePointer();
        }
    }

    @SneakyThrows
    String serializeContent (File toFolder) {
        val outputFileName = new StringBuilder()
                .append(filePointer)
                .append('-')
                .append(file.getName())
                .toString();

        val outputFile = new File(toFolder, outputFileName);

        try (val fileOutputStream = new FileOutputStream(outputFile);
             val objectOutputStream = new ObjectOutputStream(fileOutputStream)) {

            objectOutputStream.writeObject(values);
        }
        return outputFile.getAbsolutePath();
    }

}
