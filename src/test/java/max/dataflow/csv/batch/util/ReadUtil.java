package max.dataflow.csv.batch.util;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import lombok.extern.slf4j.Slf4j;
import max.dataflow.csv.batch.CsvBatchPipelineRun;

@Slf4j
public class ReadUtil {

  public static String readFile(String fileName) {
    try {
      ClassLoader classLoader = CsvBatchPipelineRun.class.getClassLoader();
      InputStream inputStream = classLoader.getResourceAsStream(fileName);
      String text;
      try (Reader reader = new InputStreamReader(inputStream)) {
        text = CharStreams.toString(reader);
      }
      return text;
    } catch (IOException e) {
      log.error("Couldn't load query from file. File: {}. Error: {}", fileName, e);
    }
    return null;
  }
}
