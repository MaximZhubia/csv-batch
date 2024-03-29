package max.dataflow.csv.batch.transforms;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@RequiredArgsConstructor
public class RetrieveCsvHeaderTransform
    extends PTransform<PCollection<String>, PCollectionView<List<String>>> {

  private static final long serialVersionUID = 1342649315456420764L;
  private final String filePath;

  @Override
  public PCollectionView<List<String>> expand(PCollection<String> input) {
    return input
        .getPipeline()
        .apply(FileIO.match().filepattern(filePath))
        .apply(FileIO.readMatches())
        .apply(Sample.any(1))
        .apply(
            ParDo.of(
                new DoFn<FileIO.ReadableFile, List<String>>() {
                  @ProcessElement
                  public void process(ProcessContext context) throws IOException {
                    try (InputStream inputStream =
                        Channels.newInputStream(Objects.requireNonNull(context.element()).open())) {
                      InputStreamReader inputStreamReader =
                          new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                      BufferedReader reader = new BufferedReader(inputStreamReader);
                      String line = reader.readLine();
                      if (StringUtils.isNotEmpty(line)) {
                        List<String> values = Arrays.asList(line.split(","));
                        log.info("Headers: {}", values);
                        context.output(values);
                      }
                    }
                  }
                }))
        .apply(View.asSingleton());
  }
}
